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


def obter_parametros_data(event):
    """Extrai ano e mês do corpo do POST (JSON)."""
    body_raw = event.get('body', '{}')
    if isinstance(body_raw, str):
        body = json.loads(body_raw)
    else:
        body = body_raw

    ano = body.get('ano', datetime.now().year)
    mes = body.get('mes', datetime.now().month)

    try:
        ano_int = int(ano)
        mes_int = int(mes)
        codigo = f"{ano_int}_{mes_int:02d}"
        return [codigo], ano_int, mes_int
    except (ValueError, TypeError):
        raise ValueError(
            f"Parâmetros inválidos: ano={ano}, mes={mes}. Devem ser números.")


def transferir_rfb_para_s3(codigo, ano_ref, mes_ref, bucket, prefixo, tentativas=3):
    """Baixa da RFB e envia para o S3 via streaming."""
    url = BASE_URL.format(codigo=codigo)
    nome_arquivo = f"estatisticas_di_{codigo}.csv"
    s3_key = f"{prefixo}/ano={ano_ref}/mes={mes_ref:02d}/{nome_arquivo}"

    for tentativa in range(1, tentativas + 1):
        try:
            logger.info(
                f"Transferindo {codigo} (Tentativa {tentativa}/{tentativas})")
            # Log da URL solicitada para facilitar rastreamento no CloudWatch
            logger.info(
                f"Solicitando URL: {url} | codigo={codigo} | tentativa={tentativa}")
            req = urllib.request.Request(
                url, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})

            with urllib.request.urlopen(req, timeout=60) as response:
                logger.info(
                    f"Resposta HTTP {getattr(response, 'status', 'unknown')} recebida para URL: {url}")
                content_length = response.getheader(
                    'Content-Length') if hasattr(response, 'getheader') else None
                if content_length:
                    logger.info(
                        f"Content-Length: {content_length} | URL: {url}")
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
                logger.warning(
                    f"Arquivo {codigo} não encontrado (404). URL: {url}")
                return None
            logger.error(f"Erro HTTP {e.code} ao acessar {url}: {e}")
        except Exception as e:
            logger.error(
                f"Erro inesperado na tentativa {tentativa} ao acessar {url}: {str(e)}")

        time.sleep(2)

    return None


def lambda_handler(event, context):
    inicio_total = time.perf_counter()

    try:
        # 1. Validação de Ambiente (Apenas Bucket e Prefixo)
        s3_bucket = os.environ.get("S3_BUCKET")
        s3_prefix = os.environ.get("S3_PREFIX")

        if not s3_bucket or not s3_prefix:
            raise ValueError(
                "As variáveis S3_BUCKET e S3_PREFIX não estão configuradas.")

        # 2. Definição do período via POST
        # Agora passamos o 'event' diretamente
        codigos, ano_ref, mes_ref = obter_parametros_data(event)

        logger.info(
            f"Iniciando Extração para {ano_ref}/{mes_ref} no Bucket: {s3_bucket}")

        arquivos_salvos = []
        arquivos_falhos = []

        # 3. Extração e Carga
        for codigo in codigos:
            s3_key = transferir_rfb_para_s3(
                codigo, ano_ref, mes_ref, s3_bucket, s3_prefix)
            if s3_key:
                arquivos_salvos.append(s3_key)
            else:
                arquivos_falhos.append(codigo)

        duracao = time.perf_counter() - inicio_total

        # 4. Resposta
        if not arquivos_salvos:
            return {
                "statusCode": 404,
                "body": json.dumps({"erro": f"Arquivo {codigos[0]} não encontrado na Receita."})
            }

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "sucesso",
                "arquivos": arquivos_salvos,
                "tempo_execucao": f"{duracao:.2f}s"
            })
        }

    except Exception as e:
        logger.exception("Erro crítico")
        return {
            "statusCode": 400,
            "body": json.dumps({"erro": str(e)})
        }
