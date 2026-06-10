# src/common/masking.py
"""
Helpers sinh SQL expression che PII (Personally Identifiable Information).
Dung trong build_mart_360 (Spark SQL) — Phase 4 Governance.

2 ky thuat:
  - Hash bat kha nghich (SHA-256): token de join/dedup ma khong lo gia tri goc (id_number).
  - Redaction mot phan (van doc duoc): che bot ky tu cho name/phone/email.

Cac ham tra ve chuoi SQL expression (chuan Spark SQL / ANSI), noi suy thang vao
cau SELECT. Khong phu thuoc PySpark -> test duoc doc lap.
"""


def hash_col(col: str) -> str:
    """Hash bat kha nghich (token) cho id_number: '079...' -> sha2 hex 64 ky tu."""
    return f"sha2(CAST({col} AS STRING), 256)"


def mask_name(col: str) -> str:
    """'Nguyen Van An' -> 'Nguyen ****' (giu ho/tu dau, che phan con lai)."""
    return (f"CASE WHEN {col} IS NULL THEN NULL "
            f"ELSE concat(split({col}, ' ')[0], ' ****') END")


def mask_phone(col: str) -> str:
    """'0912345901' -> '091****901' (giu 3 dau + 3 cuoi). Chuoi qua ngan giu nguyen."""
    return (f"CASE WHEN {col} IS NULL OR length({col}) < 6 THEN {col} "
            f"ELSE concat(substr({col}, 1, 3), '****', "
            f"substr({col}, length({col}) - 2, 3)) END")


def mask_email(col: str) -> str:
    """'user12@example.com' -> 'u***@example.com' (giu 1 ky tu dau + nguyen domain)."""
    return (f"CASE WHEN {col} IS NULL OR instr({col}, '@') = 0 THEN {col} "
            f"ELSE concat(substr({col}, 1, 1), '***', "
            f"substr({col}, instr({col}, '@'))) END")
