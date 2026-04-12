import os
import time
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BASE_URL = "https://www.gov.br/receitafederal/dados/estatisticas_di_{codigo}.csv/@@download/file"

def obter_parametros_data(meses_atras):
    """Calcula o ano, mês e o código de referência com base no atraso configurado."""
    hoje = datetime.now(timezone.utc)
    ano_ref = hoje.year
    mes_ref = hoje.month - meses_atras

    while mes_ref <= 0:
        mes_ref += 12
        ano_ref -= 1

    codigo = f"{ano_ref}_{mes_ref:02d}"
    return [codigo], ano_ref, mes_ref


def transferir_rfb_para_s3(codigo, ano_ref, mes_ref, bucket, prefixo, tentativas=3):
    """Baixa da RFB e envia para o S3 via streaming."""
    url = BASE_URL.format(codigo=codigo)
    nome_arquivo = f"estatisticas_di_{codigo}.csv"
    s3_key = f"{prefixo}/ano={ano_ref}/mes={mes_ref:02d}/{nome_arquivo}"

    for tentativa in range(1, tentativas + 1):
        try:
            logger.info(f"Transferindo {codigo} (Tentativa {tentativa}/{tentativas})")
            req = urllib.request.Request(url, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
            
            with urllib.request.urlopen(req, timeout=60) as response:
                if response.status == 200:
                    s3_client.upload_fileobj(
                        response, 
                        bucket, 
                        s3_key,
                        ExtraArgs={
                            "ContentType": "text/csv",
                            "Metadata": {
                                "codigo": codigo,
                                "origem": "rfb",
                                "gerado_em": datetime.now(timezone.utc).isoformat()
                            }
                        }
                    )
                    return s3_key
                    
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.warning(f"Arquivo {codigo} não encontrado (404).")
                return None
            logger.error(f"Erro HTTP {e.code} ao acessar {url}")
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {tentativa}: {str(e)}")
            
        time.sleep(2)
        
    return None

def lambda_handler(event, context):
    """Orquestra a extração validando as dependências primeiro."""
    inicio_total = time.perf_counter()
    request_id = context.aws_request_id if hasattr(context, 'aws_request_id') else "local-test"
    
    try:
        # 1. Validação de Ambiente
        s3_bucket = os.environ.get("S3_BUCKET")
        s3_prefix = os.environ.get("S3_PREFIX")
        meses_atras_str = os.environ.get("MESES_ATRAS")

        if not s3_bucket or not s3_prefix or not meses_atras_str:
            raise ValueError("As variáveis S3_BUCKET, S3_PREFIX e MESES_ATRAS não estão configuradas no Terraform.")

        meses_atras = int(meses_atras_str)
        
        logger.info(f"Iniciando Extração | RequestID: {request_id} | Bucket: {s3_bucket}")

        # 2. Definição do período
        codigos, ano_ref, mes_ref = obter_parametros_data(meses_atras)
        
        arquivos_salvos = []
        arquivos_falhos = []

        # 3. Extração e Carga
        for codigo in codigos:
            s3_key = transferir_rfb_para_s3(codigo, ano_ref, mes_ref, s3_bucket, s3_prefix)
            
            if s3_key:
                arquivos_salvos.append(s3_key)
            else:
                arquivos_falhos.append(codigo)

        duracao = time.perf_counter() - inicio_total

        # 4. Resposta
        if not arquivos_salvos:
            mensagem = "Nenhum arquivo foi transferido para o S3."
            logger.error(mensagem)
            return {"statusCode": 404, "body": json.dumps({"erro": mensagem})}

        resultado = {
            "statusCode": 200,
            "body": json.dumps({
                "mensagem": "Extração RAW concluída",
                "bucket": s3_bucket,
                "salvos": arquivos_salvos,
                "falhas": arquivos_falhos,
                "duracao_segundos": round(duracao, 2)
            }, ensure_ascii=False)
        }
        
        logger.info(f"Finalizado em {duracao:.2f}s | Salvos: {len(arquivos_salvos)} | Falhas: {len(arquivos_falhos)}")
        return resultado

    except Exception as e:
        logger.exception(f"Erro crítico na execução: {str(e)}")
        raise e