# Failure analysis: hanging / failing ingest CronJobs

Analysis date: 2026-10-02 (code analysis, done with Claude Code). No cluster access or logs were available, so every finding comes from reading the code and manifests. Where something needs cluster data to confirm, it is marked as such.

**Scope:** this repo (`pygeoapi_ingestor`) and the deployment repo `otc-cce-k8s-ns-dev-i-cisk`, in the state from **while the system was operational**: commit `7664675^`, when the `data-center-ingest` Deployment was still running. The commenting-out of that Deployment in `7664675` happened after the project ended and is not a cause.

**Observed behaviour:** CronJobs start and their data often ends up completely in S3, but the trigger job never finishes. It hangs or ends up `KubeJobFailed`. The heavy jobs (CDS, SPI, bias correction, GloFAS seasonal) are affected most.

---

## TL;DR

| Hypothesis | Verdict |
|---|---|
| Our async implementation / pygeoapi async | **Main cause.** Long jobs run inside gunicorn web workers that get recycled after 50 requests. |
| Race conditions | Real, but secondary. Mostly caused by the client re-POSTing the same job. |
| Too few workers | **No.** More workers only delay the problem. |
| Resources | Make it worse (CPU throttling, OOM risk), but are not the main trigger. |

---

## 1. Main cause: `--max-requests` kills running async jobs

### How it happens

1. pygeoapi 0.20 runs an async job **as a thread inside the gunicorn worker** that received the POST: `dummy.Process(target=self._execute_handler_sync, …)` in `pygeoapi/process/manager/base.py:226`. With the gevent worker class this thread is a greenlet in that worker process.
2. gunicorn counts **every request a worker handles**: `self.nr += 1; if self.nr >= self.max_requests: … "Autorestarting worker after current request."` (`gunicorn/workers/base_async.py:99-103`, gunicorn 20.1.0). The limit is `WSGI_MAX_REQUESTS=50`: the default in [entrypoint.sh:55](entrypoint.sh#L55), and set explicitly in the ingest Deployment since 2025-08-15 (`c22a97c`, raised from 5 to 50 in `2a0157a`).
3. On restart, the gevent worker only waits for open **HTTP request handlers** in its server pool, at most `graceful_timeout` (30 s, `ggevent.py:97-115`). The job greenlet is not part of that pool, so the worker process exits and **the job dies without any exception**.
4. pygeoapi creates the job with `status: accepted, progress: 5` (`base.py:430-443`). The next status update only happens **after** `p.execute()` returns (`base.py:277-290`). A killed job therefore stays `accepted` forever. Even healthy jobs show `accepted` the whole time, so a live job cannot be told apart from a dead one.
5. The TinyDB file is on a PVC, so the other workers keep answering the polls with HTTP 200 and `accepted`. The client loop in [src/invoke/utils.py:52-60](src/invoke/utils.py#L52-L60) has **no deadline**, and no CronJob sets `activeDeadlineSeconds`. The trigger pod polls forever. Because of `concurrencyPolicy: Forbid`, every later schedule of that CronJob is skipped too.

### Q&A: "Can the 51st status request not get through?"

Not quite. The status request itself still works. The job behind it is gone:

- **The counter is per worker, not per job.** gunicorn spreads requests over the 4 workers. Each worker counts *all* requests it handles: the polls from every running CronJob, POSTs, retries and any other traffic. When a job starts, the worker may already be at request 45.
- **The 50th request is still answered completely.** Only then does the worker shut down, and gunicorn starts a new one. Later polls go to other workers, are answered from the shared TinyDB file, and keep returning `accepted`, because the job thread died with the old worker process.
- **What matters is job runtime vs. the remaining lifetime of the hosting worker**, not the number of the job's own status checks. Rough numbers: one poller every 10 s spread over 4 workers is about 1 request per worker every 40 s, so a worker lives roughly **33 min**. With 4-5 parallel pollers (the evening Georgia window) it is roughly **7 min**.
- **Practical effect:** short jobs usually finish before their worker recycles. Long jobs, especially ones that wait on external services (CDS/EWDS queue, FTP, large downloads), almost certainly get killed. Waiting makes it worse: the job uses no CPU, but pollers keep pushing the request counter up.
- **Sync jobs are affected too.** The worker is recycled and the running request is killed after 30 s. The client gets a 502 or an empty response.

### Why the data can still be in S3 although the job "never finishes"

- **Duplicate executions.** The client re-POSTs on errors (see 2 and 3). Several copies of the same job run, and one of them writes the data completely. The poller is watching a different, killed job id that stays `accepted`.
- **The worker is recycled after the zarr write**, but before `execute()` returns: during the config update, metadata write or cleanup. The data is complete, but the status is never written.

### When this applies

This only applies **from 2025-08-15**, when `--max-requests` was set in the ingest Deployment. Failures before that date are explained by sections 2-4.

---

## 2. Sync cronjobs: Traefik 504 after 180 s, then re-POST, then duplicate jobs

- The client only sends `Prefer: respond-async` when `inputs.async` is the **JSON boolean** `true` ([src/invoke/utils.py:32](src/invoke/utils.py#L32)). A string `"true"` or a missing key means the job runs **synchronously**.
- **Active cronjobs without `async` (sync):** `ingest-georgia-cds-seasonal-forecast-temp`, `ingest-georgia-smhi`, `ingest-georgia-spi-forecast`, `ingest-italy-smhi`, `ingest-rijnland-knmi-evapo`, `datacreation-rijnland-knmi-smhi-forecast`, `ingest-spain-creaf-oliveoil`, `ingest-spain-creaf-projections`.
- **Cronjobs with the string `"true"`:** the 15 Spain SPI/SPEI/consec-dry-days jobs and `data-conversion-zarr-to-geotiff`, all suspended. Also `ingest-rijnland-knmi-evapo-obs-forecast` (active) from 2025-01-22 until `04be8d7` on 2025-09-18.
- The IngressRoute uses a `ServersTransport` with `responseHeaderTimeout: 180s` (`otc-cce-k8s-ns-dev-i-cisk/components/data-center/ingress.yaml@7664675^`). Any sync job longer than 3 min gets a **504**, but pygeoapi keeps running it, because WSGI cannot cancel a request when the client disconnects.
- The client treats every non-2xx as retryable and **POSTs the same job again** ([src/invoke/utils.py:79-86](src/invoke/utils.py#L79-L86)). After 3 tries it exits 52. `restartPolicy: OnFailure` restarts the container, which may POST again. The result is up to roughly 3-6 parallel executions of the same heavy job with the same `zarr_out` and the same config upsert. The CronJob ends `BackoffLimitExceeded`, even if one of the copies eventually succeeds.

## 3. The client turns transient errors into failures or false successes

All in [src/invoke/utils.py](src/invoke/utils.py):

- **A single non-2xx status poll** gives an immediate `sys.exit(52)` with no retry (lines 61-64). During a poll that lasts hours, one ingest-pod restart is enough.
- POST, the `Location` lookup and polling share **one try block**. Any exception during polling (ConnectionError, ChunkedEncodingError, JSONDecodeError) leads to **a new POST**, i.e. a duplicate job (lines 88-92).
- If the last try ends in an exception, the loop ends **without `sys.exit`**. The process exits 0, and the Job shows *Complete* although nothing was ingested.
- No `timeout=` on any `requests` call: a half-open connection hangs forever.
- The result body is never checked. Many processors return HTTP 200 / `successful` on errors (CDS `SKIPPED`/`DENIED`, KNMI `process_failed!`, SMHI/FTP errors that are swallowed).
- CronJob specs: `backoffLimit: 1` with `restartPolicy: OnFailure` fails the Job on the first restart and **deletes the pod, so its logs are lost**. `failedJobsHistoryLimit: 1` keeps only the last run.

## 4. Resources and scheduling (amplifier)

- The ingest pod has requests `4500M/500m` and limits `5500M/850m` (`deployment.yaml@7664675^`). The limits were cut from `12000M/3000m` (`1db7b4d`), via `83f3ab9`, to this value in `c22a97c`, the same commit that introduced `max-requests`.
- **One pod with 4 gevent workers runs every job in-process.** CPU-bound code (SPI gamma fits, bias correction, pandas loops) cannot be pre-empted by gevent and blocks the worker's event loop. Combined with CPU throttling at 0.85 cores, jobs run several times longer and fall more often into the windows from sections 1 and 2.
- **Evening window, 19:00-21:00 UTC, on odd days (worst on day 7):** `georgia-smhi` (sync), `georgia-spi-forecast` (sync), `georgia-spi-historic`, `georgia-cds-bias-correction`, `georgia-cds-seasonal-forecast-temp` (sync), plus the hourly `italy-rer-river-discharge-arpae`. That is up to 5 heavy jobs at the same time, with many pollers that speed up worker recycling.
- **OOM risk:** for example, KNMI obs-forecast reprojects the whole obs zarr eagerly (about 3 GB peak), and duplicate executions multiply memory use. An OOMKill takes down every job running in the pod, and all of them stay `accepted`. Whether this actually happened needs cluster data.
- The ingest pod has **no readiness, liveness or startup probes**, uses `strategy: Recreate`, `imagePullPolicy: Always` and `:latest`. Every restart opens a window of 502/503 responses, and pollers exit 52 in it.

## 5. Race conditions (real, mostly triggered by duplicates)

- `write_config` opens the file with `"w"`, which **truncates it before `flock`**, then dumps the YAML in place without a temp file and rename ([src/pygeoapi_ingestor_plugin/utils.py:100-108](src/pygeoapi_ingestor_plugin/utils.py#L100-L108)). A reader at that moment gets an empty or partial YAML, which causes a `TypeError` or YAML error and a failed job. A worker killed mid-dump can leave the serv config truncated permanently.
- Existence checks read the config **without a lock** (e.g. `ingestor_cds_process.py`, `utils_spi.py`, `georgia_cds_bias_correction_process.py`, `ingestor_knmi_evapo.py`). Several writers write **outside the lock** (`rer_river_discharge_arpae_process.py`, `georgia_ftp_alazani_shaqriani_hydro_process.py`, `georgia_cds_bias_correction_process.py`), and `ingestor_smhi_process.py` takes no lock at all. `init_main.py` rewrites the config on serv start without the lock.
- **Fixed temp paths** are shared between jobs (e.g. `/tmp/seasonal-original-single-levels.nc` for Georgia t2m and the inner bias-correction ingestions, `/tmp/data/<stem>` with `rmtree`, `/tmp/knmi`).
- Duplicate executions writing zarr with `mode='w'` delete each other's chunks. "Done" checks use `s3.exists(prefix)`, so a partial zarr counts as finished.

## 6. Smaller issues found on the way

- Cronjobs call the API through the **public hostname** (DNS, ELB hairpin, TLS, Traefik with its 180 s limit) instead of the in-cluster Service. pygeoapi builds `Location` from `server.url`, so polling also goes through Traefik.
- `--reload` / `--reload-extra-file` runs in production ([entrypoint.sh:131-132](entrypoint.sh#L131-L132), marked as a FIXME). On the serv pod, every config write by an ingestion reloads all serv workers.
- Deterministic processor bugs, independent of the infrastructure:
  - `ingestor_creaf_tif_forecast.py`: `sorted(check_collections())[0]` with no guard for an empty list.
  - ARPAE: `zero-size array` when no station data exists for that hour.
  - KNMI obs-forecast: `IndexError` when the SMHI forecast does not extend past the last observation.
  - `utils_spi.check_s3_path_exists` dereferences `None`.
- After commit `0c6ff2f` renamed most CronJobs, old `ingest-job-*` CronJobs may still exist in the cluster, because `kubectl apply -k` runs without `--prune`. Unverified.

---

## Recommended fixes

### Quick wins

1. Ingest Deployment: `WSGI_MAX_REQUESTS=0` (or remove `--max-requests`), and remove `--reload`.
2. Every CronJob: `activeDeadlineSeconds` (2-6 h depending on the process), `startingDeadlineSeconds`, `restartPolicy: Never` with `backoffLimit: 0` (the client already retries), and a higher `failedJobsHistoryLimit`.
3. Client (`src/invoke/utils.py`):
   - Always send `Prefer: respond-async` (all processors support it), or at least parse `str(v).lower() == "true"`.
   - **Never re-POST** once a job may have been accepted.
   - Retry polls with backoff on 5xx and connection errors, within a total wait limit (`INVOKE_MAX_WAIT_SECONDS`).
   - Set `timeout=` on every request.
   - Always exit non-zero when the loop ends without success, and check the result body.
4. On ingest start, mark leftover `accepted` / `running` TinyDB jobs as `failed`.
5. Ingest pod: readiness and startup probes (`GET /ingest/processes?f=json`), and a pinned image tag.

### Medium term

- One config helper for every reader and writer: `FileLock` for the whole read-modify-write, writing to `<config>.tmp`, then `fsync` and `os.replace`. Also use it in `init_main.py` and the SMHI processor.
- Per-execution temp directories (`tempfile.mkdtemp`). Write zarr to a temp prefix or write a completion marker last.
- Processors raise `ProcessorExecuteError` on real failures instead of returning HTTP 200 / `SKIPPED`.
- Spread out the evening schedules. While jobs still run in-process: Guaranteed QoS with at least 8 Gi and 2-3 CPU.

### Structural fix

Do not run long jobs inside long-lived web workers. Run **one Kubernetes Job per execution**: `pygeoapi-k8s-manager`, prepared in `components/k8s-job-manager` but never deployed, or the CronJob pod running the processor directly. Use PostgreSQL instead of TinyDB for the job store. The pod's exit code is then the real result, and OOMs and timeouts become visible per job.

---

## How to confirm (if the cluster or PVCs still exist)

```shell
# hanging trigger jobs (ACTIVE set, old lastScheduleTime)
kubectl -n dev-i-cisk get cronjob -o custom-columns=NAME:.metadata.name,SUSPEND:.spec.suspend,ACTIVE:.status.active[*].name,LAST:.status.lastScheduleTime,LAST_OK:.status.lastSuccessfulTime

# worker recycling in the ingest logs
kubectl -n dev-i-cisk logs deploy/data-center-ingest | grep -E 'Autorestarting worker|Worker exiting|Booting worker|graceful timeout|WORKER TIMEOUT'

# OOM / restarts
kubectl -n dev-i-cisk get pod -l app=data-center-ingest -o jsonpath='{..lastState}'
```

Best remaining evidence: `jobs.db` on PVC `dev-i-cisk-data-center-ingest-jobs-database`. Count the jobs stuck in `accepted` (progress 5) created after 2025-08-15, and the duplicates of the same process created about 190 s apart. Duplicates confirm section 2, stuck `accepted` jobs confirm section 1.
