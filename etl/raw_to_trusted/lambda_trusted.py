import os
import urllib.parse
from io import StringIO
import boto3
import pandas as pd

s3_client = boto3.client('s3')


def extrair_dados_do_evento(event):
    """Extrai o nome do bucket de origem e o caminho do arquivo do gatilho do S3."""
    bucket = event['Records'][0]['s3']['bucket']['name']
    chave = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    return bucket, chave

def ler_csv_do_s3(bucket, chave):
    """Faz o download do arquivo S3 e retorna o conteúdo em texto."""
    print(f"Lendo arquivo bruto: s3://{bucket}/{chave}")
    response = s3_client.get_object(Bucket=bucket, Key=chave)
    return response['Body'].read().decode('utf-8')

def limpar_e_transformar_dados(dados_brutos):
    """Contém toda a regra de negócio da Brink Calçados usando Pandas."""
    
    # Nota: mantive o seu sep='@', certifique-se de que o dado Raw usa esse separador. 
    # Se der erro, troque para sep=';' ou sep=','
    df = pd.read_csv(StringIO(dados_brutos), sep='@')
    print(f"Shape original (Raw): {df.shape}")

    # 1. Padroniza colunas
    df.columns = df.columns.str.lower().str.strip()
    
    # 2. Regras de negócio (Formatação)
    if 'ncm' in df.columns:
        df['ncm'] = df['ncm'].astype(str).str.strip().str.zfill(8)
        
    if 'pais_origem' in df.columns:
        df['pais_origem'] = df['pais_origem'].astype(str).str.strip().str.upper()

    # Filtro A: Apenas NCMs do capítulo 64 (Calçados)
    if 'ncm' in df.columns:
        df = df[df['ncm'].str.startswith('64')]
        
    # Filtro B: Ignorar amostras minúsculas (Volume > 100)
    # Procuramos pela coluna de volume (ajuste o nome se o seu CSV usar outro)
    coluna_volume = 'qtd_ume_soma_truncado'
    if coluna_volume in df.columns:
        # Garante que números com vírgula do padrão brasileiro virem float do Python
        if df[coluna_volume].dtype == object:
            df[coluna_volume] = df[coluna_volume].str.replace(',', '.').astype(float)
        
        # Mantém apenas transações com mais de 100 unidades
        df = df[df[coluna_volume] > 100]

    print(f"Shape após filtros da Brink (Trusted): {df.shape}")

    # 4. Retorna o CSV limpo (o to_csv padrão usa vírgula como separador)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()

def salvar_csv_no_s3(bucket_destino, chave_origem, conteudo_csv):
    """Gera a nova rota e salva o arquivo tratado no bucket Trusted."""

    chave_destino = chave_origem.replace("importacao/", "trusted/").replace(".csv", "_trusted.csv")
    
    s3_client.put_object(
        Bucket=bucket_destino,
        Key=chave_destino,
        Body=conteudo_csv
    )
    print(f"✔ Salvo com sucesso em: s3://{bucket_destino}/{chave_destino}")
    return chave_destino


def lambda_handler(event, context):
    """Orquestra a execução das funções."""
    try:
        # 1. Validação de Ambiente
        trusted_bucket = os.environ.get("BUCKET_TRUSTED")
        if not trusted_bucket:
            raise ValueError("Variável BUCKET_TRUSTED não configurada no Terraform.")

        # 2. Extração (Origem)
        raw_bucket, chave_origem = extrair_dados_do_evento(event)
        
        # 3. Leitura (I/O)
        dados_brutos = ler_csv_do_s3(raw_bucket, chave_origem)
        
        # 4. Transformação (Regra de Negócio)
        dados_limpos = limpar_e_transformar_dados(dados_brutos)
        
        # 5. Carga (I/O Destino)
        caminho_final = salvar_csv_no_s3(trusted_bucket, chave_origem, dados_limpos)

        return {
            "statusCode": 200,
            "status": "success", 
            "destination": caminho_final
        }

    except Exception as e:
        print(f"Erro no pipeline Raw -> Trusted: {str(e)}")
        raise e