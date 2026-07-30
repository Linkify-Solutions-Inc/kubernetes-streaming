# Module 8 — RDS Postgres

← [Module 7: Secrets](07-secrets.md) · [Index](README.md) · [Module 9: S3 and CloudFront](09-s3-and-cloudfront.md) →

---

## What you're building

A managed PostgreSQL 16 instance in the private subnets, reachable only from the EKS cluster, with its master password generated straight into Secrets Manager so it never touches your terminal. Then the part that actually takes thought: getting the four tables into it. RDS has no `/docker-entrypoint-initdb.d`, so `postgres/init.sql` — which today is the only thing that has ever created your schema — will never run. You replace it with a Kubernetes Job that applies numbered SQL files from git.

At the end, `\dt` inside the cluster lists `streamers`, `streams`, `videos`, `view_events`.

Budget: about **$12/month** for `db.t4g.micro` plus ~$1.60 for 20 GiB of gp3. Storage and backups are the parts that keep billing after you stop for the day; the instance is the part you can stop.

---

## Why it works this way

### Why the instance is this small

`db.t4g.micro` is 2 vCPU and 1 GiB on Graviton, $0.016/hr. Look at what the application actually asks a database to do: a handful of single-row lookups per HTTP request, one `INSERT` per view event, and a status update per stream. There is no join deeper than one foreign key and no table that will exceed a few thousand rows this year. `db.t3.micro` costs about 25% more for less performance, and Graviton is safe here because there are no container images involved — you are not building anything for this CPU, you are talking to it over TCP.

20 GiB is the *minimum* gp3 allocation on RDS; you cannot ask for less. Turn on storage autoscaling with a 100 GiB ceiling as free insurance against `view_events` running away.

Single-AZ. Multi-AZ doubles the bill for a failover you will never exercise on a learning cluster, and the honest consequence — the database is unavailable during maintenance windows and instance replacement — is something you should experience once rather than pay to hide.

Engine version **16.x**, because compose runs `postgres:16-alpine` and you do not want to discover a syntax difference during Module 13. One thing that migrating-from-old-Postgres guides get wrong: `gen_random_uuid()` has been built in since PostgreSQL 13, so `001_baseline.sql` needs **no `CREATE EXTENSION pgcrypto`** and no superuser.

### Why the security group references another security group, not a CIDR

RDS gets its own security group whose only ingress rule is "TCP 5432 from the EKS cluster security group."

That matters more than it looks. EKS attaches the cluster security group to every managed-node-group node *and* to every node Karpenter launches, automatically, forever. So one rule written against that SG covers nodes that do not exist yet and will be created and destroyed hundreds of times. A CIDR rule (`10.0.0.0/16`) would work today and would also let anything else in the VPC reach your database, including a compromised pod in a namespace you have not created yet.

There is a VPC CNI consequence worth understanding, because it is why you do not need Security Groups for Pods: with the AWS VPC CNI, pods get real IPs from the node's subnet and inherit the node's security groups. "The pod can reach RDS" follows from "the node can reach RDS." Nothing extra to configure.

### Why `--manage-master-user-password`

The alternative is `--master-user-password 'hunter2'` on the command line, which puts a production credential in your shell history, in your terminal scrollback, and in whatever process listing anyone on the machine runs. `--manage-master-user-password` has RDS generate the password itself, store it in Secrets Manager, and never show it to you. You cannot leak what you never saw.

This is also what makes Module 7's wiring pay off. External Secrets Operator already knows how to pull from Secrets Manager; RDS's managed secret is just another secret to pull. The DSN gets assembled by an ESO template, so the password exists in exactly one place and rotating it is an RDS operation, not a git commit.

### Why `rds.force_ssl` is worth a paragraph

RDS PostgreSQL 15 and later default `rds.force_ssl` to `1`, and you should leave it there. The subtlety is how it fails, which is: **it doesn't**. Every service in this codebase does `psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=3)`, and psycopg3 defaults to `sslmode=prefer`. `prefer` negotiates TLS successfully against a force_ssl server, so everything appears to work — while doing zero certificate verification. You get encryption without authentication and no error message telling you so.

Put `?sslmode=require` in the DSN explicitly. It does not change the wire behaviour; it changes whether the next person reading the DSN can tell what the connection guarantees. The step beyond, `sslmode=verify-full`, needs the RDS CA bundle mounted into every pod plus `sslrootcert=` in the DSN — worth knowing about, not worth doing on this project.

### Why the schema bootstrap is a Job

This is the real content of the module, so be clear about what changed.

In compose, `postgres/init.sql` is bind-mounted to `/docker-entrypoint-initdb.d/init.sql`. The official Postgres image runs everything in that directory exactly once, on first startup against an empty data directory. That hook is a feature of the *image*, not of Postgres. RDS does not use that image. There is no hook. If you create the instance and deploy the services, every query fails with `relation "streamers" does not exist` and nothing in the logs will point at the missing hook, because from the application's point of view the database is up and answering.

Three ways to fix it:

- **Run `psql` by hand once.** RDS is in a private subnet with no public access, so this needs a bastion host or a port-forward through a pod. It works, it is not reproducible, and in four months nobody will remember it happened or what was in it. Rejected.
- **An init container on each Deployment.** Five services would race to create the same four tables on every rollout. One would win; four would crash-loop with `relation "streamers" already exists`. Rejected.
- **A Kubernetes Job, in git, idempotent, re-runnable.** This one.

The Job mounts a ConfigMap of SQL files and loops over them with `psql -v ON_ERROR_STOP=1`. The ConfigMap is *generated by Kustomize from `postgres/migrations/*.sql`*, which is the load-bearing detail: the SQL lives in one place in the repo, it is diffable in a pull request, and there is no copy of it pasted into YAML that will drift.

### Why the migrations are a convention and not a framework

The Job applies **every** file in `postgres/migrations/` on **every** run, in lexical order. There is no ledger table, no version tracking, no down-migrations. That works only because every statement is written to be re-runnable: `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`.

Be honest about what you are giving up:

- **No version table**, so the Job cannot tell you which migrations have been applied, and cannot detect that someone applied `003` to production before `002`.
- **No rollback.** Getting a bad migration out means writing `004_undo_003.sql`.
- **No support for a non-idempotent change.** A data backfill (`UPDATE videos SET ...`) would re-run on every deploy. A column rename has no `IF NOT EXISTS` form. If you need either, this convention does not stretch — stop and adopt Alembic or Atlas.

Why accept that? Four tables, one repository, and a team small enough to talk to each other. A migration framework here is more machinery to learn than schema to manage. And the escape hatch is cheap: numbered SQL files in a directory is exactly the input format Flyway, Atlas and Alembic's raw-SQL mode already expect, so adopting one later is a config change, not a rewrite.

One rule to internalise now, because it prevents the most common self-inflicted outage in this design: **a migration must be compatible with the image that is currently running.** In [Module 15](15-argocd-gitops.md) this Job becomes its own ArgoCD Application at sync wave 25, one wave ahead of the application at 30 — meaning it runs *before* the new pods exist, while the old pods are still serving traffic. So: add columns with defaults, never drop or rename in the same release as the code change. Drop in a follow-up release, after nothing references the column.

---

## Do it

### 1. Confirm the repo is ready

[Module 0](00-preflight-code-changes.md) moved the schema to its new home and made it re-runnable. Check that it happened, because everything below reads from that directory:

```sh
ls postgres/migrations/
grep -c "IF NOT EXISTS" postgres/migrations/001_baseline.sql
```

```
001_baseline.sql
4
```

Four `IF NOT EXISTS` — one per table. If `postgres/init.sql` still exists and `postgres/migrations/` does not, go back and do Module 0 section 0.4 first; the rest of this module has nothing to apply.

The Job manifests are already in the repo at `k8s/apps/base/db-migrate/`.

### 2. Find the network

Everything below derives from the cluster, so there is nothing to copy by hand:

```sh
export AWS_PROFILE=linkify-streaming
export AWS_REGION=us-east-1

VPC_ID=$(aws eks describe-cluster --name streaming \
  --query 'cluster.resourcesVpcConfig.vpcId' --output text)

CLUSTER_SG=$(aws eks describe-cluster --name streaming \
  --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)

# The private subnets are the ones Module 2 tagged for internal load balancers.
PRIVATE_SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" \
            "Name=tag:kubernetes.io/role/internal-elb,Values=1" \
  --query 'Subnets[].SubnetId' --output text)

echo "vpc=$VPC_ID"
echo "cluster sg=$CLUSTER_SG"
echo "private subnets=$PRIVATE_SUBNETS"
```

`CLUSTER_SG` is the value the whole security model hangs on. It is the security group EKS creates and attaches to every node — its group name looks like `eks-cluster-sg-streaming-1234567890`. If that echo prints `None`, the cluster name is wrong or your profile is pointed at the wrong account; fix it before continuing.

### 3. Subnet group and security group

```sh
aws rds create-db-subnet-group \
  --db-subnet-group-name streaming-db \
  --db-subnet-group-description "streaming private subnets" \
  --subnet-ids $PRIVATE_SUBNETS
```

RDS requires the subnet group to span at least two Availability Zones even for a single-AZ instance. That is an API requirement, not a hint that you should go Multi-AZ.

```sh
RDS_SG=$(aws ec2 create-security-group \
  --group-name streaming-rds \
  --description "RDS Postgres, reachable from the EKS cluster only" \
  --vpc-id "$VPC_ID" \
  --query GroupId --output text)

aws ec2 authorize-security-group-ingress \
  --group-id "$RDS_SG" \
  --protocol tcp --port 5432 \
  --source-group "$CLUSTER_SG"

echo "rds sg=$RDS_SG"
```

That is the only ingress rule this security group will ever have. Confirm it:

```sh
aws ec2 describe-security-groups --group-ids "$RDS_SG" \
  --query 'SecurityGroups[0].IpPermissions'
```

Expect exactly one permission, port 5432, with a `UserIdGroupPairs` entry naming `$CLUSTER_SG` and **an empty `IpRanges`**. A non-empty `IpRanges` means you have a CIDR rule you did not intend.

### 4. Parameter group

You cannot modify a default parameter group, so make your own:

```sh
aws rds create-db-parameter-group \
  --db-parameter-group-name streaming-pg16 \
  --db-parameter-group-family postgres16 \
  --description "streaming"

aws rds modify-db-parameter-group \
  --db-parameter-group-name streaming-pg16 \
  --parameters \
    "ParameterName=rds.force_ssl,ParameterValue=1,ApplyMethod=pending-reboot" \
    "ParameterName=log_min_duration_statement,ParameterValue=1000,ApplyMethod=immediate" \
    "ParameterName=log_connections,ParameterValue=1,ApplyMethod=immediate"
```

`log_min_duration_statement=1000` logs any query over one second. On this workload nothing should ever appear there, so anything that does is a real finding. `log_connections` is on because every handler in this codebase opens a fresh connection per request — when connection count becomes a problem, this log is how you will see it.

### 5. Create the instance

```sh
aws rds create-db-instance \
  --db-instance-identifier streaming-db \
  --engine postgres \
  --engine-version 16.10 \
  --db-instance-class db.t4g.micro \
  --allocated-storage 20 \
  --max-allocated-storage 100 \
  --storage-type gp3 \
  --db-name streaming \
  --master-username streamadmin \
  --manage-master-user-password \
  --db-subnet-group-name streaming-db \
  --vpc-security-group-ids "$RDS_SG" \
  --db-parameter-group-name streaming-pg16 \
  --backup-retention-period 7 \
  --no-multi-az \
  --no-publicly-accessible \
  --deletion-protection \
  --no-enable-performance-insights \
  --tags Key=Project,Value=streaming
```

Two things to check rather than trust:

```sh
# Which 16.x versions this region will actually accept today.
aws rds describe-db-engine-versions --engine postgres \
  --query 'DBEngineVersions[?starts_with(EngineVersion, `16.`)].EngineVersion' \
  --output text
```

If `16.10` is not in that list, use the highest 16.x that is. Do not jump to 17 — compose runs 16.

Note `--deletion-protection`. It is on deliberately, and it means Module 16's teardown has an extra step (`aws rds modify-db-instance --no-deletion-protection`) before the delete will work. That friction is the point.

Creation takes 5–10 minutes:

```sh
aws rds wait db-instance-available --db-instance-identifier streaming-db
```

### 6. Read the endpoint and the secret ARN

```sh
DB_HOST=$(aws rds describe-db-instances --db-instance-identifier streaming-db \
  --query 'DBInstances[0].Endpoint.Address' --output text)

DB_SECRET_ARN=$(aws rds describe-db-instances --db-instance-identifier streaming-db \
  --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text)

DB_SECRET_NAME=$(aws secretsmanager describe-secret --secret-id "$DB_SECRET_ARN" \
  --query Name --output text)

echo "host=$DB_HOST"
echo "secret=$DB_SECRET_NAME"
```

Now look at what is actually inside that secret, because the answer decides how you write the ESO template and it is a common source of a broken DSN:

```sh
aws secretsmanager get-secret-value --secret-id "$DB_SECRET_ARN" \
  --query SecretString --output text | python3 -m json.tool | sed 's/"password".*/"password": "REDACTED"/'
```

For an RDS-managed master user secret this is **just `username` and `password`** — the endpoint, port and database name are not in it, because RDS considers those instance metadata rather than credentials. (Secrets that people create by hand with the "credentials for RDS database" template *do* carry `host`/`port`/`dbname`, which is where the confusion comes from.) Those two keys are also the only ones AWS documents: the API contract covers the secret's ARN and its status, not its JSON shape.

**Which is exactly why the ESO template does not read the endpoint from it.** Referencing a key that is not there is not an error in ESO — the template renders an empty string, the `ExternalSecret` reports `SecretSynced`, and you get `postgresql://user:pw@:5432/?sslmode=require` in a Secret that looks perfectly healthy. Betting the DSN on an undocumented field, where being wrong produces a green checkmark, is the wrong bet.

### 6b. Record the endpoint as its own Secrets Manager entry

The host, port and database name are not credentials — they are stable facts about an instance sitting in a private subnet. But [Module 7](07-secrets.md)'s template wants one uniform source, so put them where it can fetch them:

```sh
aws secretsmanager create-secret \
  --name streaming/db-endpoint \
  --description "Non-secret RDS connection facts for the ESO template" \
  --secret-string "{\"host\":\"$DB_HOST\",\"port\":\"5432\",\"dbname\":\"streaming\"}"
```

Three notes on that:

- **`dbname` is `streaming`** because that is what `--db-name streaming` created in step 5. It is not the master username.
- **It is under the `streaming/` prefix**, so Module 7's IAM policy — which already allows `streaming/*` — covers it with no change. Nothing to add.
- **If you ever restore this instance from a snapshot under a new identifier, the endpoint changes.** Update this secret with `aws secretsmanager put-secret-value --secret-id streaming/db-endpoint --secret-string '{...}'` and ESO picks it up within the hour. That is a one-line fix in one place, which is the reason for keeping it here rather than baking the hostname into the manifest.

Confirm it reads back:

```sh
aws secretsmanager get-secret-value --secret-id streaming/db-endpoint \
  --query SecretString --output text
```

```
{"host":"streaming-db.abc123xyz.us-east-1.rds.amazonaws.com","port":"5432","dbname":"streaming"}
```

### 7. Wire it to ESO

This uses the `ClusterSecretStore` from [Module 7](07-secrets.md). Check the CRD's served API version first rather than assuming — ESO promoted these types from `v1beta1` to `v1` and both may be present:

```sh
kubectl get crd externalsecrets.external-secrets.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
```

Use the version with `storage=true`. The manifest itself already exists — `k8s/infra/secrets/externalsecret-db.yaml`, written and explained in [Module 7](07-secrets.md). Do not write a second one. It has exactly one placeholder, because the RDS-managed secret's name is generated rather than chosen:

```sh
sed -i.bak "s|rds!db-REPLACE-ME|$DB_SECRET_NAME|" k8s/infra/secrets/externalsecret-db.yaml
rm k8s/infra/secrets/externalsecret-db.yaml.bak

grep -A1 'extract:' k8s/infra/secrets/externalsecret-db.yaml
```

```
    - extract:
        key: rds!db-a1b2c3d4-5e6f-7890-abcd-ef1234567890
--
    - extract:
        key: streaming/db-endpoint
```

Two `extract` entries, and that is the whole point of the design: `username` and `password` from the secret RDS owns and rotates, `host`/`port`/`dbname` from the entry you created in step 6b. ESO merges both into one map and the template in that file composes the DSN from it.

`| urlquery` in that template is not optional. RDS-generated passwords contain punctuation, and an unescaped `/`, `@` or `#` inside a DSN silently truncates the connection string into something that points at the wrong host or database.

The resulting DSN has this shape — this is what every service will read as `POSTGRES_DSN`:

```
postgresql://streamadmin:<generated>@streaming-db.abc123xyz.us-east-1.rds.amazonaws.com:5432/streaming?sslmode=require
```

**No IAM change is needed here.** Module 7's policy already lists two resource patterns — `arn:aws:secretsmanager:us-east-1:${ACCOUNT_ID}:secret:streaming/*` and `arn:aws:secretsmanager:us-east-1:${ACCOUNT_ID}:secret:rds!db-*` — which is exactly the two secrets above: `streaming/db-endpoint` matches the first, the RDS-managed secret matches the second. Nor is a `kms:Decrypt` grant required: the managed secret is encrypted with the AWS-managed `aws/secretsmanager` key, and access to that key is granted implicitly by the Secrets Manager permission. (If you ever move the secret to a **customer-managed** KMS key, that stops being true and you would add `kms:Decrypt` on that key's ARN — but nothing in this course does.)

Worth re-reading the policy once rather than taking that on trust:

```sh
aws iam get-role-policy --role-name streaming-external-secrets \
  --policy-name read-streaming-secrets --query 'PolicyDocument.Statement[].Resource'
```

Apply and confirm the Secret materialises:

```sh
kubectl apply -f k8s/infra/secrets/externalsecret-db.yaml
kubectl -n streaming get externalsecret streaming-db
```

```
NAME           STORE                STATUS         READY
streaming-db   aws-secretsmanager   SecretSynced   True
```

`READY False` here is an IAM problem 90% of the time; `kubectl -n streaming describe externalsecret streaming-db` prints the AWS error verbatim.

### 8. Run the migration Job

The manifests are at `k8s/apps/base/db-migrate/`. Look at them before running:

- `job.yaml` — a `postgres:16-alpine` pod that loops over `/migrations/*.sql` with `ON_ERROR_STOP=1` and `--single-transaction`, reading `POSTGRES_DSN` from the Secret ESO just created.
- `kustomization.yaml` — a `configMapGenerator` that builds the ConfigMap from `../../../../postgres/migrations/001_baseline.sql`.

That relative path reaches outside the kustomization directory, which Kustomize refuses by default. Build with the restrictor relaxed:

```sh
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/apps/base/db-migrate \
  | kubectl apply -f -
```

Watch it:

```sh
kubectl -n streaming wait --for=condition=complete job/db-migrate --timeout=180s
kubectl -n streaming logs job/db-migrate
```

```
applying migrations from /migrations
--> 001_baseline.sql
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
migrations complete
            List of relations
 Schema |    Name     | Type  |   Owner
--------+-------------+-------+------------
 public | streamers   | table | streamadmin
 public | streams     | table | streamadmin
 public | videos      | table | streamadmin
 public | view_events | table | streamadmin
(4 rows)
```

Run it a second time to prove it is re-runnable. A Job's pod template is immutable, so re-running means deleting first:

```sh
kubectl -n streaming delete job db-migrate
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/apps/base/db-migrate \
  | kubectl apply -f -
kubectl -n streaming wait --for=condition=complete job/db-migrate --timeout=180s
```

The log now shows four `NOTICE: relation "streamers" already exists, skipping` lines instead of `CREATE TABLE`, and the Job still completes. That is the whole idempotency argument, demonstrated rather than asserted.

### 9. Adding a migration later

The convention, in full: a new numbered file in `postgres/migrations/`, every statement re-runnable, and no edits to a file that has already been applied anywhere.

```sh
# Illustration only -- do NOT commit this one. 002 is spoken for.
cat > /tmp/00N_example.sql <<'SQL'
ALTER TABLE streams ADD COLUMN IF NOT EXISTS example_note TEXT;
CREATE INDEX IF NOT EXISTS streams_status_idx ON streams (status);
SQL
```

Commit a real one and re-run the Job (Module 15 makes this automatic on every sync). The new file changes the ConfigMap's content hash, which changes the Job's pod template, which is how ArgoCD knows there is work to do.

**`002_transcode_claim.sql` is already reserved.** [Module 14](14-keda-scaledjobs.md) adds it — `transcode_status` and `transcode_heartbeat` on `streams`, `transcode_heartbeat` on `videos`, and a partial index on `streams (status)` — because the one-Job-per-stream model needs a claim and a heartbeat lease that this schema does not have yet. Write it there, with the content Module 14 gives you, not here. This matters more than it looks: `ADD COLUMN IF NOT EXISTS` is a no-op once the column exists, so a placeholder `transcode_status TEXT` created now would silently keep Module 14's `NOT NULL DEFAULT 'pending'` from ever taking effect, and nothing would report an error.

---

## Verify

The checkpoint is: **the four tables exist, and you can see them from inside the cluster over TLS.**

```sh
kubectl -n streaming run psql --rm -it --restart=Never --image=postgres:16-alpine \
  --overrides='
{
  "spec": {
    "containers": [{
      "name": "psql",
      "image": "postgres:16-alpine",
      "stdin": true, "tty": true,
      "command": ["sh","-c","exec psql \"$POSTGRES_DSN\""],
      "env": [{"name":"POSTGRES_DSN","valueFrom":{"secretKeyRef":{"name":"streaming-db","key":"POSTGRES_DSN"}}}]
    }]
  }
}'
```

The `--overrides` form is used instead of `--env` on purpose: it keeps the DSN out of your shell history and out of the pod spec, where `kubectl get pod -o yaml` would show it to anyone with namespace read access.

At the prompt:

```
psql (16.10)
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, ...)
Type "help" for help.

streaming=> \dt
             List of relations
 Schema |    Name     | Type  |    Owner
--------+-------------+-------+-------------
 public | streamers   | table | streamadmin
 public | streams     | table | streamadmin
 public | videos      | table | streamadmin
 public | view_events | table | streamadmin
(4 rows)

streaming=> \d streams
                    Table "public.streams"
   Column    |           Type           | Nullable |     Default
-------------+--------------------------+----------+------------------
 id          | uuid                     | not null | gen_random_uuid()
 streamer_id | uuid                     | not null |
 path        | text                     | not null |
 status      | text                     | not null | 'live'::text
 started_at  | timestamp with time zone | not null | now()
 ended_at    | timestamp with time zone |          |

streaming=> \q
```

Three things in that output are the actual checkpoint, not decoration:

1. **`SSL connection (protocol: TLSv1.3 ...)`** on the banner line — `rds.force_ssl` is doing its job and your DSN is not silently falling back.
2. **Four tables.**
3. **`gen_random_uuid()` as a default** — proof that PG16's built-in UUID function resolved without `pgcrypto`.

Do not move to Module 9 until all three are true.

---

## What breaks

Ordered by how often it actually happens.

### 1. `security; file ... is not in or below ...` on `kubectl apply -k`

```
error: loading KV pairs: file sources: [../../../../postgres/migrations/001_baseline.sql]:
security; file '/…/postgres/migrations/001_baseline.sql' is not in or below
'/…/k8s/apps/base/db-migrate'
```

Kustomize refuses to read files outside the kustomization root unless told otherwise. This is the first thing you will hit, and `kubectl apply -k` has no flag for it — you have to build and pipe:

```sh
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/apps/base/db-migrate | kubectl apply -f -
```

In Module 15, ArgoCD needs the same permission, set once in the `argocd-cm` ConfigMap:

```yaml
data:
  kustomize.buildOptions: --load-restrictor LoadRestrictionsNone
```

If relaxing it bothers you, the alternative is to move the SQL to `k8s/apps/base/db-migrate/sql/` and give up on `postgres/migrations/` being the shared home for compose and EKS. A symlink does **not** work — Kustomize resolves the symlink and then rejects the resolved path.

### 2. The ExternalSecret is stuck `READY False`

```sh
kubectl -n streaming describe externalsecret streaming-db
```

Look at the Events. Three errors dominate:

- `ResourceNotFoundException` on `rds!db-REPLACE-ME` — step 7's `sed` did not run, or ran against a different copy of the file. `grep REPLACE-ME k8s/infra/secrets/externalsecret-db.yaml` answers it in one line.
- `ResourceNotFoundException` on `streaming/db-endpoint` — you skipped step 6b. Create it and the next refresh succeeds.
- `AccessDeniedException ... secretsmanager:GetSecretValue` — genuinely IAM, but check the *name* before you touch the policy. Module 7's role already allows both `streaming/*` and `rds!db-*`, so this normally means the Pod Identity association is missing or the secret is in another region, not that a permission is absent. `aws iam get-role-policy --role-name streaming-external-secrets --policy-name read-streaming-secrets` shows you what is actually allowed.

**A synced ExternalSecret is not proof of a correct one.** If the template referenced a key that no source provides, ESO renders it as an empty string and still reports `SecretSynced`. Decode the result rather than trusting the status:

```sh
kubectl -n streaming get secret streaming-db -o jsonpath='{.data.POSTGRES_DSN}' | base64 -d; echo
```

An `@:5432` with nothing between the `@` and the colon means the host came out empty — the `streaming/db-endpoint` entry is missing a key or has it spelled differently.

After changing anything, force a re-sync rather than waiting an hour:

```sh
kubectl -n streaming annotate externalsecret streaming-db force-sync="$(date +%s)" --overwrite
```

### 3. The Job pod hangs, then fails with `connection timeout expired`

Almost always the security group. Confirm the rule references the cluster SG:

```sh
aws ec2 describe-security-groups --group-ids "$RDS_SG" \
  --query 'SecurityGroups[0].IpPermissions[0].UserIdGroupPairs'
```

Then confirm the RDS instance is actually using that SG — it is easy to create the group and forget to attach it:

```sh
aws rds describe-db-instances --db-instance-identifier streaming-db \
  --query 'DBInstances[0].VpcSecurityGroups'
```

If both look right, check the endpoint resolves from inside the cluster:

```sh
kubectl -n streaming run dns --rm -it --restart=Never --image=busybox:1.36 -- \
  nslookup streaming-db.abc123xyz.us-east-1.rds.amazonaws.com
```

An RDS endpoint resolves to a **private** 10.x address. If it resolves to a public address, `--no-publicly-accessible` was not applied.

Note that this same class of failure has a nastier form later: the services use `connect_timeout=3`, which was fine against a Postgres container on the same Docker bridge and is aggressive against RDS across an AZ boundary. It hits `ingest-webhook`'s `/hooks/auth` path first, and a timeout there means **MediaMTX rejects the publish** — the stream never starts, and the error looks like an OBS problem. Watch for it in Module 13.

### 4. `relation "streamers" already exists` and the Job goes `Failed`

A migration is not idempotent. Find the offending statement:

```sh
kubectl -n streaming logs job/db-migrate | tail -20
```

`ON_ERROR_STOP=1` means the file that printed last is the file that failed. Every `CREATE` needs `IF NOT EXISTS`. If you hit this on a statement that has no `IF NOT EXISTS` form, that is the signal described above — you have outgrown the convention for that particular change, and wrapping it in a `DO $$ ... EXCEPTION WHEN duplicate_object THEN NULL; $$` block is a warning sign, not a solution.

### 5. `psql: FATAL: no pg_hba.conf entry ... no encryption`

Something is connecting with `sslmode=disable`. Check the DSN that actually landed in the Secret:

```sh
kubectl -n streaming get secret streaming-db -o jsonpath='{.data.POSTGRES_DSN}' \
  | base64 -d | sed 's#://[^@]*@#://***@#'
```

The redaction keeps the password out of your scrollback while still showing you whether `?sslmode=require` survived the template. A common cause is a stray newline: use `>-` (fold, strip trailing newline) in the ESO template, not `>` or `|`.

### 6. `FATAL: too many connections`

Every request handler in this codebase opens a fresh `psycopg.connect()` with no pool, and now pays a TLS handshake for each one. `db.t4g.micro` allows roughly 85 connections. Two `web` + two `upload-api` + two `ingest-webhook` + readiness probes are comfortable; add eight concurrent transcode Jobs in Module 14 and it gets close.

```sh
aws rds describe-db-instances --db-instance-identifier streaming-db \
  --query 'DBInstances[0].DBInstanceClass'
```

The fix is `psycopg_pool.ConnectionPool` in the services, which is a follow-up and not a blocker. If you need relief today, scale replicas down rather than scaling the instance up.

---

Next: [Module 9 — S3 and CloudFront](09-s3-and-cloudfront.md), where the same "it worked in compose because the dev tool was permissive" pattern shows up twice more.
