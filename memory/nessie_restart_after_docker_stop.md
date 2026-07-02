---
name: nessie-restart-after-docker-stop
description: Sau khi tắt/bật lại Docker, Nessie 0.79.0 có thể fail mọi commit (POST) — restart riêng Nessie là khắc phục
metadata:
  type: project
---

Sau khi `docker compose down`/tắt Docker rồi bật lại, **Nessie 0.79.0 dễ vào trạng thái lỗi commit**: mọi POST commit từ Spark fail, còn GET (đọc) vẫn 200.

**Triệu chứng phía Spark/bronze:**
```
org.projectnessie.client.http.HttpClientException: Failed to execute POST ... /api/v2/trees/main@<hash>/contents
Caused by: java.io.IOException: HTTP/1.1 header parser received no bytes
Caused by: java.io.EOFException: EOF reached while reading
```
**Nguyên nhân gốc (log phía Nessie):** bug tầng HTTP Vert.x/Quarkus —
`java.lang.NullPointerException: ... "this.pending" is null` tại
`io.vertx.core.http.impl.Http1xServerRequest.handleException` → Nessie trả HTTP 500 và đóng kết nối không gửi byte nào. Chỉ dính request **có body** (POST commit); request không body (GET /config, GET tree) vẫn OK → **đọc Gold qua Trino vẫn chạy nhưng ghi mới fail**.

**Khắc phục (đã kiểm chứng 2026-06-11):** `docker restart lakehouse-nessie`, chờ `GET /api/v2/config` trả 200, rồi chạy lại pipeline. Catalog KHÔNG mất data vì store là ROCKSDB bền (xem [[phase3_done.md]], khác với gotcha IN_MEMORY ở [[phase2_done.md]]). Sau restart bronze chạy lại thành công ~2 phút (trước đó fail kèm 3 lần retry ~11 phút).

**How to apply:** nếu bronze fail với "EOF / header parser received no bytes" sau khi vừa bật Docker, đừng debug code pipeline — restart Nessie trước.
