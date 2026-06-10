# scripts/generate_data.py
"""
Sinh du lieu gia lap cho Lakehouse Customer 360.

2 mode:
  --mode bootstrap : TRUNCATE + nap 800 KH + lich su 18 thang (chay 1 lan, dat nen).
  --mode daily     : sinh data 1 ngay (giao dich moi + KH moi + doi segment + NPL),
                     APPEND/UPDATE (khong wipe). Airflow truyen --date {{ ds }}.

Chay duoc CA host LAN trong Docker nho ENV:
  - Trong Docker (mac dinh): noi service name postgres / oracle-xe.
  - Tren host: set PG_HOST=localhost  ORA_DSN=localhost:1521/XEPDB1.

Vi du:
  # bootstrap tren host
  PG_HOST=localhost ORA_DSN=localhost:1521/XEPDB1 python scripts/generate_data.py --mode bootstrap
  # daily trong container
  docker exec lakehouse-data-simulator python /app/scripts/generate_data.py --mode daily --date 2026-06-02

GHI CHU CDC quan trong:
  Cot ky thuat created_at/updated_at = NOW()/SYSTIMESTAMP (gio THUC), KHONG phai --date.
  Ly do: incremental_load loc `incremental_col > last_run_time` (gio thuc cua lan chay truoc).
  Neu set created_at = ngay mo phong (qua khu) thi se BI LOC BO. Chi cot nghiep vu
  txn_date/txn_datetime/opened_date moi = ngay mo phong (--date).
"""
import os
import random
import argparse
import datetime as dt

from dotenv import load_dotenv
import psycopg2
import oracledb

load_dotenv()

# ── Ket noi (host tu ENV) ────────────────────────────────────────────────────
PG_HOST    = os.getenv("PG_HOST", "postgres")
PG_PORT    = int(os.getenv("PG_PORT", "5432"))
ORA_DSN    = os.getenv("ORA_DSN", "oracle-xe:1521/XEPDB1")
SIM_CONFIG = os.getenv("SIM_CONFIG", os.path.join(os.path.dirname(__file__), "..", "config", "simulation.yaml"))


def pg_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname="banking",
                            user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])


def ora_conn():
    return oracledb.connect(user=os.environ["ORACLE_USER"],
                            password=os.environ["ORACLE_PASSWORD"], dsn=ORA_DSN)


# ── Hang so dung chung ───────────────────────────────────────────────────────
AS_OF        = dt.date(2025, 12, 31)             # moc bootstrap
WINDOW_START = AS_OF - dt.timedelta(days=540)    # ~18 thang lich su giao dich
N_CUST       = 800

HO  = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Dang", "Bui", "Do",
       "Ho", "Ngo", "Duong", "Ly"]
DEM = ["Van", "Thi", "Hoang", "Minh", "Thanh", "Quoc", "Huu", "Ngoc", "Gia", "Duc", "Kim", "Hong"]
TEN = ["An", "Bich", "Cuong", "Dung", "Em", "Phuong", "Giang", "Hanh", "Khanh", "Linh",
       "Mai", "Nam", "Oanh", "Phuc", "Quan", "Son", "Trang", "Tuan", "Vinh", "Yen"]

SEGMENTS = ["MASS"] * 70 + ["AFFLUENT"] * 22 + ["PREMIER"] * 8
ALL_SEGMENTS = ["MASS", "AFFLUENT", "PREMIER"]
CITIES = [("Ho Chi Minh", "Ho Chi Minh"), ("Ha Noi", "Ha Noi"), ("Da Nang", "Da Nang"),
          ("Can Tho", "Can Tho"), ("Hai Phong", "Hai Phong"), ("Nha Trang", "Khanh Hoa")]
MCC = ["GROCERY", "DINING", "TRAVEL", "SHOPPING", "FUEL", "ENTERTAINMENT", "HEALTH", "EDUCATION"]
BANK_CHANNELS = ["ATM", "ONLINE", "POS", "BRANCH"]
CARD_CHANNELS = ["POS", "ONLINE"]
BANK_TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]
NPL_STATES = ["SUB_STD", "DOUBTFUL", "OVERDUE"]   # NPL_STATUS la VARCHAR2(10)

SEG_MULT = {"MASS": 1.0, "AFFLUENT": 4.0, "PREMIER": 12.0}


def rand_date(start: dt.date, end: dt.date) -> dt.date:
    return start + dt.timedelta(days=random.randint(0, (end - start).days))


# ══════════════════════════════════════════════════════════════════════════════
# BOOTSTRAP — sinh toan bo tu dau (TRUNCATE + nap)
# ══════════════════════════════════════════════════════════════════════════════
def gen_customers():
    custs = []
    for i in range(1, N_CUST + 1):
        cif = f"CIF{i:06d}"
        name = f"{random.choice(HO)} {random.choice(DEM)} {random.choice(TEN)}"
        dob = dt.date(random.randint(1955, 2003), random.randint(1, 12), random.randint(1, 28))
        gender = random.choice(["M", "F"])
        id_number = f"{79000000000 + i:012d}"
        phone = f"09{random.randint(10**7, 10**8 - 1)}"
        email = f"user{i}@example.com"
        city, prov = random.choice(CITIES)
        seg = random.choice(SEGMENTS)
        custs.append((cif, name, dob, gender, id_number, phone, email, city, prov, seg, "VERIFIED"))
    return custs


def gen_bank_accounts(custs):
    accts = []
    seq = 0
    for c in custs:
        cif, seg = c[0], c[9]
        mult = SEG_MULT[seg]
        n = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        for k in range(n):
            seq += 1
            acc_id = f"BACC{seq:06d}"
            if k == 0:
                acc_type, product, interest = "CURRENT", "PRD003", 0
            elif random.random() < 0.5:
                acc_type, product, interest = "TERM_DEPOSIT", "PRD001", 5.5
            else:
                acc_type, product, interest = "CURRENT", "PRD003", 0
            branch = random.choice(["BR001", "BR002"])
            balance = round(random.uniform(1_000_000, 50_000_000) * mult, 2)
            opened = rand_date(dt.date(2018, 1, 1), AS_OF - dt.timedelta(days=60))
            account_no = f"00{seq:08d}"
            accts.append((acc_id, cif, product, branch, acc_type, account_no,
                          balance, "ACTIVE", opened, interest))
    return accts


def gen_bank_txns(accts):
    txns = []
    seq = 0
    for a in accts:
        acc_id, balance = a[0], a[6]
        for _ in range(random.randint(0, 40)):
            seq += 1
            d = rand_date(WINDOW_START, AS_OF)
            ttime = dt.datetime.combine(d, dt.time(random.randint(6, 22), random.randint(0, 59)))
            ttype = random.choice(BANK_TXN_TYPES)
            amount = round(random.uniform(50_000, 20_000_000), 2)
            balance_after = round(balance + random.uniform(-amount, amount), 2)
            channel = random.choice(BANK_CHANNELS)
            txns.append((f"BTXN{seq:08d}", acc_id, d, ttime, ttype, amount, balance_after, channel))
    return txns


def gen_card_accounts(custs):
    cards = []
    seq = 0
    for c in custs:
        cif, seg = c[0], c[9]
        if random.random() > 0.55:
            continue
        seq += 1
        acc_id = f"CARD{seq:06d}"
        card_type = random.choice(["VISA_CREDIT", "MASTERCARD_CREDIT", "VISA_DEBIT"])
        card_number = f"4{random.randint(10**14, 10**15 - 1)}"
        if "CREDIT" in card_type:
            credit_limit = round(random.uniform(20_000_000, 100_000_000) * SEG_MULT[seg], 2)
            outstanding = round(random.uniform(0, credit_limit * 0.6), 2)
            due = AS_OF + dt.timedelta(days=random.randint(5, 25))
        else:
            credit_limit, outstanding, due = None, 0, None
        opened = rand_date(dt.date(2018, 1, 1), AS_OF - dt.timedelta(days=60))
        cards.append((acc_id, cif, card_type, card_number, credit_limit, outstanding, due, "ACTIVE", opened))
    return cards


def gen_card_txns(cards):
    txns = []
    seq = 0
    for cd in cards:
        acc_id = cd[0]
        for _ in range(random.randint(0, 30)):
            seq += 1
            d = rand_date(WINDOW_START, AS_OF)
            ttime = dt.datetime.combine(d, dt.time(random.randint(6, 23), random.randint(0, 59)))
            amount = round(random.uniform(50_000, 10_000_000), 2)
            mcc = random.choice(MCC)
            channel = random.choice(CARD_CHANNELS)
            txns.append((f"CTXN{seq:08d}", acc_id, d, ttime, amount, "PURCHASE",
                         f"Merchant {mcc.title()}", mcc, channel))
    return txns


def gen_loans(custs):
    loans = []
    seq = 0
    for c in custs:
        cif = c[0]
        if random.random() > 0.30:
            continue
        seq += 1
        loan_id = f"LOAN{seq:06d}"
        principal = round(random.uniform(20_000_000, 500_000_000), 2)
        outstanding = round(principal * random.uniform(0.2, 1.0), 2)
        disb = rand_date(dt.date(2021, 1, 1), AS_OF - dt.timedelta(days=30))
        is_npl = random.random() < 0.08
        if is_npl:
            npl = random.choice(NPL_STATES)
            maturity = AS_OF - dt.timedelta(days=random.randint(10, 200))
        else:
            npl = "NORMAL"
            maturity = disb + dt.timedelta(days=random.randint(365, 1825))
        loans.append((loan_id, cif, "PRD002", random.choice(["BR001", "BR002"]), "PERSONAL",
                      principal, outstanding, 12.5, disb, maturity, npl))
    return loans


def run_bootstrap():
    random.seed(42)
    print("Generating (bootstrap) ...")
    custs = gen_customers()
    bank_accts = gen_bank_accounts(custs)
    bank_txns = gen_bank_txns(bank_accts)
    cards = gen_card_accounts(custs)
    card_txns = gen_card_txns(cards)
    loans = gen_loans(custs)
    print(f"  customers={len(custs)} bank_accts={len(bank_accts)} bank_txns={len(bank_txns)}")
    print(f"  cards={len(cards)} card_txns={len(card_txns)} loans={len(loans)}")

    # Postgres
    pg = pg_conn(); pgc = pg.cursor()
    pgc.execute("TRUNCATE card_transactions, card_accounts, customers RESTART IDENTITY CASCADE")
    pgc.executemany(
        "INSERT INTO customers (customer_id,full_name,date_of_birth,gender,id_number,phone,email,"
        "city,province,customer_segment,kyc_status,created_at,updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())", custs)
    pgc.executemany(
        "INSERT INTO card_accounts (account_id,customer_id,card_type,card_number,credit_limit,"
        "outstanding_balance,due_date,status,opened_date,closed_date,created_at,updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NOW(),NOW())", cards)
    pgc.executemany(
        "INSERT INTO card_transactions (txn_id,account_id,txn_date,txn_datetime,amount,currency,"
        "txn_type,merchant_name,merchant_category,channel,status,created_at) "
        "VALUES (%s,%s,%s,%s,%s,'VND',%s,%s,%s,%s,'COMPLETED',NOW())", card_txns)
    pg.commit(); pgc.close(); pg.close()

    # Oracle
    ora = ora_conn(); orac = ora.cursor()
    for t in ["bank_transactions", "loans", "bank_accounts"]:
        orac.execute(f"DELETE FROM {t}")
    orac.executemany(
        "INSERT INTO bank_accounts (account_id,customer_id,product_id,branch_id,account_type,"
        "account_no,balance,currency,status,opened_date,closed_date,interest_rate,updated_at) "
        "VALUES (:1,:2,:3,:4,:5,:6,:7,'VND',:8,:9,NULL,:10,SYSTIMESTAMP)", bank_accts)
    orac.executemany(
        "INSERT INTO loans (loan_id,customer_id,product_id,branch_id,loan_type,principal_amount,"
        "outstanding_amount,interest_rate,disbursement_date,maturity_date,npl_status,status,updated_at) "
        "VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,'ACTIVE',SYSTIMESTAMP)", loans)
    orac.executemany(
        "INSERT INTO bank_transactions (txn_id,account_id,txn_date,txn_datetime,txn_type,amount,"
        "balance_after,channel,status,created_at) "
        "VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'COMPLETED',SYSTIMESTAMP)", bank_txns)
    ora.commit(); orac.close(); ora.close()
    print("DONE bootstrap. AS_OF =", AS_OF)


# ══════════════════════════════════════════════════════════════════════════════
# DAILY — sinh data 1 ngay (append/update, idempotent)
# ══════════════════════════════════════════════════════════════════════════════
def run_daily(date: dt.date, cfg: dict):
    random.seed(int(date.strftime("%Y%m%d")))
    ymd = date.strftime("%Y%m%d")
    slow, fast = cfg["slow"], cfg["fast"]

    pg = pg_conn(); pgc = pg.cursor()
    ora = ora_conn(); orac = ora.cursor()

    # --- Idempotency guard: da co data ngay nay thi skip (DAG retry an toan) ---
    orac.execute("SELECT COUNT(*) FROM bank_transactions WHERE txn_date = :1", [date])
    if orac.fetchone()[0] > 0:
        print(f"[daily {date}] da ton tai bank_transactions ngay nay -> SKIP (idempotent)")
        pgc.close(); pg.close(); orac.close(); ora.close()
        return

    # --- Doc state hien co ---
    pgc.execute("SELECT customer_id, customer_segment FROM customers")
    customers = pgc.fetchall()
    pgc.execute("SELECT account_id FROM card_accounts WHERE status = 'ACTIVE'")
    card_accts = [r[0] for r in pgc.fetchall()]
    orac.execute("SELECT account_id, balance FROM bank_accounts WHERE status = 'ACTIVE'")
    bank_accts = orac.fetchall()
    orac.execute("SELECT loan_id FROM loans WHERE status = 'ACTIVE'")
    loan_ids = [r[0] for r in orac.fetchall()]

    # --- TIER FAST: bank transactions (txn_date = ngay mo phong) ---
    bank_txns = []; seq = 0
    for acc_id, balance in bank_accts:
        if random.random() > fast["account_txn_participation"]:
            continue
        for _ in range(random.randint(*fast["bank_txns_per_account"])):
            seq += 1
            amount = round(random.uniform(50_000, 20_000_000), 2)
            bal_after = round(float(balance) + random.uniform(-amount, amount), 2)
            ttime = dt.datetime.combine(date, dt.time(random.randint(6, 22), random.randint(0, 59)))
            bank_txns.append((f"BTXN{ymd}{seq:06d}", acc_id, date, ttime,
                              random.choice(BANK_TXN_TYPES), amount, bal_after, random.choice(BANK_CHANNELS)))

    # --- TIER FAST: card transactions ---
    card_txns = []; seq = 0
    for acc_id in card_accts:
        if random.random() > fast["account_txn_participation"]:
            continue
        for _ in range(random.randint(*fast["card_txns_per_account"])):
            seq += 1
            amount = round(random.uniform(50_000, 10_000_000), 2)
            mcc = random.choice(MCC)
            ttime = dt.datetime.combine(date, dt.time(random.randint(6, 23), random.randint(0, 59)))
            card_txns.append((f"CTXN{ymd}{seq:06d}", acc_id, date, ttime, amount, "PURCHASE",
                              f"Merchant {mcc.title()}", mcc, random.choice(CARD_CHANNELS)))

    # --- TIER SLOW: KH moi (+1 CURRENT account, doi khi them card). ID co tien to ngay ---
    new_custs, new_bank, new_cards = [], [], []
    for k in range(random.randint(*slow["new_customers_per_day"])):
        cif = f"CIFN{ymd}{k:03d}"
        seg = random.choice(SEGMENTS)
        city, prov = random.choice(CITIES)
        name = f"{random.choice(HO)} {random.choice(DEM)} {random.choice(TEN)}"
        dob = dt.date(random.randint(1960, 2004), random.randint(1, 12), random.randint(1, 28))
        new_custs.append((cif, name, dob, random.choice(["M", "F"]), f"99{ymd}{k:04d}",
                          f"09{random.randint(10**7, 10**8 - 1)}", f"{cif.lower()}@example.com",
                          city, prov, seg, "VERIFIED"))
        new_bank.append((f"BACN{ymd}{k:03d}", cif, "PRD003", random.choice(["BR001", "BR002"]),
                         "CURRENT", f"9{ymd}{k:04d}",
                         round(random.uniform(1_000_000, 50_000_000) * SEG_MULT[seg], 2),
                         "ACTIVE", date, 0))
        if random.random() < slow["new_card_prob"]:
            ctype = random.choice(["VISA_CREDIT", "MASTERCARD_CREDIT", "VISA_DEBIT"])
            if "CREDIT" in ctype:
                climit = round(random.uniform(20_000_000, 100_000_000) * SEG_MULT[seg], 2)
                cout, cdue = round(random.uniform(0, climit * 0.4), 2), date + dt.timedelta(days=random.randint(5, 25))
            else:
                climit, cout, cdue = None, 0, None
            new_cards.append((f"CARN{ymd}{k:03d}", cif, ctype, f"4{random.randint(10**14, 10**15 - 1)}",
                              climit, cout, cdue, "ACTIVE", date))

    # --- TIER SLOW: doi segment (kich hoat SCD2) + chuyen NPL ---
    seg_changes = [(random.choice([s for s in ALL_SEGMENTS if s != seg]), cid)
                   for cid, seg in customers if random.random() < slow["customer_change_rate"]]
    npl_changes = [(random.choice(NPL_STATES), lid)
                   for lid in loan_ids if random.random() < slow["npl_transition_rate"]]

    # --- GHI (conflict-safe). created_at/updated_at = NOW()/SYSTIMESTAMP (gio THUC) ---
    pgc.executemany(
        "INSERT INTO customers (customer_id,full_name,date_of_birth,gender,id_number,phone,email,"
        "city,province,customer_segment,kyc_status,created_at,updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW()) ON CONFLICT (customer_id) DO NOTHING", new_custs)
    pgc.executemany(
        "INSERT INTO card_accounts (account_id,customer_id,card_type,card_number,credit_limit,"
        "outstanding_balance,due_date,status,opened_date,closed_date,created_at,updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NOW(),NOW()) ON CONFLICT (account_id) DO NOTHING", new_cards)
    pgc.executemany(
        "INSERT INTO card_transactions (txn_id,account_id,txn_date,txn_datetime,amount,currency,"
        "txn_type,merchant_name,merchant_category,channel,status,created_at) "
        "VALUES (%s,%s,%s,%s,%s,'VND',%s,%s,%s,%s,'COMPLETED',NOW()) ON CONFLICT (txn_id) DO NOTHING", card_txns)
    pgc.executemany(
        "UPDATE customers SET customer_segment=%s, updated_at=NOW() WHERE customer_id=%s", seg_changes)

    orac.executemany(
        "INSERT INTO bank_accounts (account_id,customer_id,product_id,branch_id,account_type,"
        "account_no,balance,currency,status,opened_date,closed_date,interest_rate,updated_at) "
        "VALUES (:1,:2,:3,:4,:5,:6,:7,'VND',:8,:9,NULL,:10,SYSTIMESTAMP)", new_bank, batcherrors=True)
    orac.executemany(
        "INSERT INTO bank_transactions (txn_id,account_id,txn_date,txn_datetime,txn_type,amount,"
        "balance_after,channel,status,created_at) "
        "VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'COMPLETED',SYSTIMESTAMP)", bank_txns, batcherrors=True)
    orac.executemany(
        "UPDATE loans SET npl_status=:1, updated_at=SYSTIMESTAMP WHERE loan_id=:2", npl_changes)

    pg.commit(); ora.commit()
    pgc.close(); pg.close(); orac.close(); ora.close()
    print(f"[daily {date}] bank_txns={len(bank_txns)} card_txns={len(card_txns)} "
          f"new_cust={len(new_custs)} new_card={len(new_cards)} "
          f"seg_changes={len(seg_changes)} npl_changes={len(npl_changes)}")


# ══════════════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["bootstrap", "daily"], default="bootstrap")
    ap.add_argument("--date", default=dt.date.today().isoformat(), help="ngay mo phong (daily), YYYY-MM-DD")
    args = ap.parse_args()

    if args.mode == "bootstrap":
        run_bootstrap()
    else:
        import yaml
        with open(SIM_CONFIG) as f:
            cfg = yaml.safe_load(f)
        run_daily(dt.date.fromisoformat(args.date), cfg)


if __name__ == "__main__":
    main()
