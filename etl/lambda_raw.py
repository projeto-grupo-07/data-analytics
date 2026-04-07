"""
Lambda: Baixar CSVs de Estatísticas de Declarações de Importação (RFB)
e salvar consolidado no S3 (bucket raw).

Variáveis de ambiente:
    S3_BUCKET       Nome do bucket S3 (ex: meu-bucket-raw)
    S3_PREFIX       Prefixo/pasta no S3 (ex: importacao/consolidado)
    ANO_INICIO      Ano de início (default: 2021)
    MES_INICIO      Mês de início (default: 5)
    MODO            "full" = todos os meses | "incremental" = só mês atual (default: incremental)
"""

import os
import time
import json
import logging
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path


logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


BASE_URL = "https://www.gov.br/receitafederal/dados/estatisticas_di_{codigo}.csv/@@download/file"


def carregar_env_local(caminhos_relativos=(".env", "etl/.env")):
    """Carrega variaveis de ambiente de um arquivo .env local, se existir."""
    for caminho_relativo in caminhos_relativos:
        arquivo_env = Path(__file__).resolve().parent / caminho_relativo
        if not arquivo_env.exists():
            continue

        with arquivo_env.open(encoding="utf-8") as arquivo:
            for linha in arquivo:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue

                chave, valor = linha.split("=", 1)
                chave = chave.strip()
                valor = valor.strip().strip('"').strip("'")

                if chave and chave not in os.environ:
                    os.environ[chave] = valor

        break


carregar_env_local()

S3_BUCKET = os.environ.get("S3_BUCKET", "meu-bucket-raw")
S3_PREFIX = os.environ.get("S3_PREFIX", "importacao")
MESES_ATRAS = int(os.environ.get("MESES_ATRAS", "2"))

# BASE_URL = "https://www.gov.br/receitafederal/dados/estatisticas_di_{codigo}.csv/@@download/file"
# S3_BUCKET = "brinks-bucket-raw"
# S3_PREFIX = "importacao"
# ANO_INICIO = 2021
# MES_INICIO = 5


def obter_request_id(context):
    """Retorna o request_id da Lambda quando disponivel."""
    if context is None:
        return None
    if isinstance(context, dict):
        return context.get("aws_request_id") or context.get("request_id")
    return getattr(context, "aws_request_id", None)


def formatar_duracao(segundos):
    """Formata duracao em segundos com 3 casas decimais."""
    return f"{segundos:.3f}s"


def gerar_codigos():
    """Gera o codigo do mes configurado por atraso em relacao ao UTC atual."""
    logger.info("Função gerar_codigos() - iniciada")

    hoje = datetime.now(timezone.utc)
    ano_ref = hoje.year
    mes_ref = hoje.month - MESES_ATRAS

    while mes_ref <= 0:
        mes_ref += 12
        ano_ref -= 1

    codigos = [f"{ano_ref}_{mes_ref:02d}"]

    logger.info("Códigos gerados (MESES_ATRAS=%s): %s", MESES_ATRAS, codigos)
    return codigos, ano_ref, mes_ref


def baixar_e_salvar_stream(codigo, ano_ref, mes_ref, tentativas=3):
    """Baixa o CSV da RFB via streaming e salva diretamente no S3."""
    logger.info(f"Função baixar_e_salvar_stream() - iniciada para {codigo}")
    s3 = boto3.client("s3")
    url = BASE_URL.format(codigo=codigo)
    
    hoje = datetime.now(timezone.utc)
    nome_arquivo = f"estatisticas_di_{codigo}.csv"
    s3_key = f"{S3_PREFIX}/ano={ano_ref}/mes={mes_ref:02d}/{nome_arquivo}"

    for i in range(tentativas):
        try:
            logger.info(f"Tentativa {i+1}/{tentativas} para {codigo} | url={url}")
            req = urllib.request.Request(
                url, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"}
            )
            
            # Com o 'with', a conexão fica aberta. NÃO usamos .read() aqui.
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status == 200:
                    # O S3 puxa os dados do 'resp' diretamente, aos poucos.
                    s3.upload_fileobj(
                        resp, 
                        S3_BUCKET, 
                        s3_key,
                        ExtraArgs={
                            "ContentType": "text/csv",
                            "Metadata": {
                                "codigo": codigo,
                                "origem": "rfb",
                                "gerado_em": hoje.isoformat()
                            }
                        }
                    )
                    logger.info("✓ Salvo via streaming no S3: s3://%s/%s", S3_BUCKET, s3_key)
                    return s3_key
                    
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.info(f"- {codigo} não encontrado (404)")
                return None
            logger.warning(f"! {codigo} HTTP {e.code}, tentativa {i+1}")
        except Exception as e:
            logger.warning(f"! {codigo} erro: {e}, tentativa {i+1}")
        time.sleep(2)
        
    return None


def salvar_raw_s3(codigo, conteudo_bytes, ano_ref, mes_ref):
    s3 = boto3.client("s3")
    hoje = datetime.now(timezone.utc)
    nome_arquivo = f"estatisticas_di_{codigo}.csv"
    s3_key = f"{S3_PREFIX}/ano={ano_ref}/mes={mes_ref:02d}/{nome_arquivo}" 

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=conteudo_bytes,
        ContentType="text/csv",
        Metadata={
            "codigo": codigo,
            "origem": "rfb",
            "gerado_em": hoje.isoformat()
        },
    )
    logger.info("Salvo raw no S3: s3://%s/%s", S3_BUCKET, s3_key)
    return s3_key


def lambda_handler(event, context):
    inicio_total = time.perf_counter()
    request_id = obter_request_id(context)
    logger.info(
        "Iniciando | request_id=%s | BUCKET=%s | PREFIX=%s | event_type=%s | event_keys=%s",
        request_id,
        S3_BUCKET,
        S3_PREFIX,
        type(event).__name__,
        list(event.keys()) if isinstance(event, dict) else None,
    )

    inicio_codigos = time.perf_counter()
    codigos, ano_ref, mes_ref = gerar_codigos()
    logger.info(
        "Meses a processar: %s | total=%s | tempo=%s",
        codigos,
        len(codigos),
        formatar_duracao(time.perf_counter() - inicio_codigos),
    )

    s3_keys = []
    baixados = []
    nao_encontrados = []

    inicio_downloads = time.perf_counter()
    for indice, codigo in enumerate(codigos, start=1):
        logger.info("Processando arquivo %s/%s | codigo=%s", indice, len(codigos), codigo)

        # Chama a função unificada de download e upload via streaming
        s3_key = baixar_e_salvar_stream(codigo, ano_ref, mes_ref)
        
        if s3_key:
            s3_keys.append(s3_key)
            baixados.append(codigo)
        else:
            nao_encontrados.append(codigo)

        time.sleep(0.5)

    logger.info(
        "Etapa finalizada | baixados=%s | nao_encontrados=%s | salvos_s3=%s | tempo_total=%s",
        len(baixados),
        len(nao_encontrados),
        len(s3_keys),
        formatar_duracao(time.perf_counter() - inicio_downloads),
    )

    if not s3_keys:
        msg = "Nenhum arquivo baixado/salvo"
        logger.error("%s | request_id=%s", msg, request_id)
        return {"statusCode": 404, "body": msg}

    resultado = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "bucket": S3_BUCKET,
                "prefixo": S3_PREFIX,
                "arquivos_salvos": s3_keys,
                "baixados": baixados,
                "nao_encontrados": nao_encontrados,
            },
            ensure_ascii=False,
        ),
    }

    logger.info(
        "Resultado final | request_id=%s | status=%s | baixados=%s | nao_encontrados=%s | salvos_s3=%s | duracao_total=%s",
        request_id,
        resultado.get("statusCode"),
        len(baixados),
        len(nao_encontrados),
        len(s3_keys),
        formatar_duracao(time.perf_counter() - inicio_total),
    )
    return resultado


if __name__ == "__main__":    # Para testes locais
    logger.info("Executando lambda_handler()")
    resultado = lambda_handler({}, {})
    logger.info("Resultado da execução local:")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
