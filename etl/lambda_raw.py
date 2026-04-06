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
import io
import csv
import time
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = "https://www.gov.br/receitafederal/dados/estatisticas_di_{codigo}.csv/@@download/file"
S3_BUCKET = os.environ.get("S3_BUCKET", "meu-bucket-raw")
S3_PREFIX = os.environ.get("S3_PREFIX", "importacao/consolidado")
ANO_INICIO = int(os.environ.get("ANO_INICIO", "2021"))
MES_INICIO = int(os.environ.get("MES_INICIO", "5"))
MODO = os.environ.get("MODO", "incremental")  # "full" ou "incremental"


def gerar_codigos():
    """Gera lista de códigos ano_mes conforme o MODO."""
    hoje = datetime.today()

    if MODO == "incremental":
        # Só o mês anterior (mais seguro, dados do mês atual podem estar incompletos)
        if hoje.month == 1:
            return [f"{hoje.year - 1}_12"]
        return [f"{hoje.year}_{hoje.month - 1:02d}"]

    # MODO full: todos os meses desde ANO_INICIO/MES_INICIO
    codigos = []
    for ano in range(ANO_INICIO, hoje.year + 1):
        mes_ini = MES_INICIO if ano == ANO_INICIO else 1
        mes_fim = hoje.month - 1 if ano == hoje.year else 12
        for mes in range(mes_ini, mes_fim + 1):
            codigos.append(f"{ano}_{mes:02d}")
    return codigos


def baixar_csv(codigo, tentativas=3):
    """Baixa um CSV da RFB. Retorna bytes ou None."""
    url = BASE_URL.format(codigo=codigo)
    for i in range(tentativas):
        try:
            req = urllib.request.Request(url, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status == 200:
                    conteudo = resp.read()
                    if len(conteudo) > 100:
                        logger.info(f"✓ {codigo} ({len(conteudo) // 1024} KB)")
                        return conteudo
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.info(f"- {codigo} não encontrado (404)")
                return None
            logger.warning(f"! {codigo} HTTP {e.code}, tentativa {i+1}")
        except Exception as e:
            logger.warning(f"! {codigo} erro: {e}, tentativa {i+1}")
        time.sleep(2)
    return None


def parsear_csv(conteudo_bytes, codigo):
    """Parseia CSV com separador @ e adiciona coluna ano_mes."""
    linhas = []
    texto = conteudo_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(texto), delimiter="@")
    cabecalho = None
    for i, row in enumerate(reader):
        if i == 0:
            cabecalho = ["ano_mes"] + row
            continue
        if row:
            linhas.append([codigo] + row)
    return cabecalho, linhas


def consolidar_e_salvar_s3(dfs_data):
    """
    dfs_data: list of (cabecalho, linhas)
    Gera CSV em memória e faz upload para S3.
    """
    s3 = boto3.client("s3")
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")

    cabecalho_escrito = False
    total_linhas = 0

    for cabecalho, linhas in dfs_data:
        if not cabecalho_escrito:
            writer.writerow(cabecalho)
            cabecalho_escrito = True
        writer.writerows(linhas)
        total_linhas += len(linhas)

    hoje = datetime.today()
    if MODO == "incremental":
        nome_arquivo = f"importacao_{hoje.year}_{hoje.month - 1:02d}.csv"
    else:
        nome_arquivo = f"importacao_consolidado_full_{hoje.strftime('%Y%m%d')}.csv"

    s3_key = f"{S3_PREFIX}/{nome_arquivo}"

    conteudo_bytes = buffer.getvalue().encode("utf-8-sig")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=conteudo_bytes,
        ContentType="text/csv",
        Metadata={
            "total_linhas": str(total_linhas),
            "gerado_em": hoje.isoformat(),
            "modo": MODO,
        },
    )

    logger.info(f"✓ Salvo no S3: s3://{S3_BUCKET}/{s3_key} ({total_linhas:,} linhas)")
    return s3_key, total_linhas


def lambda_handler(event, context):
    logger.info(f"Iniciando | MODO={MODO} | BUCKET={S3_BUCKET} | PREFIX={S3_PREFIX}")

    codigos = gerar_codigos()
    logger.info(f"Meses a processar: {codigos}")

    dfs_data = []
    baixados = []
    nao_encontrados = []

    for codigo in codigos:
        conteudo = baixar_csv(codigo)
        if conteudo:
            try:
                cabecalho, linhas = parsear_csv(conteudo, codigo)
                dfs_data.append((cabecalho, linhas))
                baixados.append(codigo)
            except Exception as e:
                logger.error(f"Erro ao parsear {codigo}: {e}")
        else:
            nao_encontrados.append(codigo)
        time.sleep(0.5)

    if not dfs_data:
        msg = "Nenhum arquivo baixado com sucesso."
        logger.error(msg)
        return {"statusCode": 404, "body": msg}

    s3_key, total_linhas = consolidar_e_salvar_s3(dfs_data)

    resultado = {
        "statusCode": 200,
        "body": json.dumps({
            "s3_uri": f"s3://{S3_BUCKET}/{s3_key}",
            "baixados": baixados,
            "nao_encontrados": nao_encontrados,
            "total_linhas": total_linhas,
            "modo": MODO,
        }, ensure_ascii=False),
    }

    logger.info(f"Resultado: {resultado}")
    return resultado