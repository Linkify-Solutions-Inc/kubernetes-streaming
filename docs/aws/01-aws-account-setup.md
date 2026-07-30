# Module 1 — AWS account, access, and not getting a surprise bill

## What you're building

A way to get an AWS credential onto your laptop that expires, is protected by MFA, and never touches a git repository — plus the two pieces of cost tooling that have to exist *before* you create anything that bills. At the end, `aws sts get-caller-identity` returns your account and role, `aws budgets` has an alarm pointed at a real inbox, and the CLI tools every later module depends on are installed at pinned versions.

Nothing in this module costs money. Everything after it does.

## Why it works this way

### IAM Identity Center, not an IAM user

The obvious path is: create an IAM user, generate an access key pair, paste it into `~/.aws/credentials`, done. Do not do this.

An IAM access key is a **permanent credential**. It does not expire. Once created it exists in `~/.aws/credentials`, then in a CI secret, then in a `.env` on a server, then in a Slack message, and eventually in a git commit. That is not a hypothetical failure mode for this project — the repo's own `.env` on the dev box has `SERVER_PASSWORD` sitting in plaintext right now. Long-lived credentials leak because there is nothing about them that forces them not to.

**IAM Identity Center** (the service formerly called AWS SSO) issues short-lived credentials instead. You authenticate in a browser, with MFA, and the CLI receives a session that expires — typically 8 hours. If the cached session file leaks tomorrow, it's already useless. There is no key to rotate because there is no key.

The operational difference you'll feel: once a day you run `aws sso login`, a browser tab opens, you approve it, and every `aws`/`eksctl`/`kubectl` command works for the rest of the day. When the session expires, commands fail with `The SSO session associated with this profile has expired` and you log in again. That's the whole cost.

### Why `AdministratorAccess` is the right permission set here

This will look wrong if you've read anything about least privilege, so here is the reasoning.

Building this platform touches roughly 40 IAM actions across a dozen services: EKS, EC2, VPC, IAM itself, CloudFormation, RDS, S3, CloudFront, Route 53, ACM, Secrets Manager, ELB. The failure mode of a too-tight policy is a CloudFormation stack that runs for twelve minutes and then rolls back with `AccessDenied` on an action you cannot easily identify from the error. You would lose days to that, and you'd learn nothing about EKS in the process.

The security controls that actually matter for a *human building infrastructure* are: MFA enforced, credentials that expire, CloudTrail recording what happened, and a budget alarm. Identity Center gives you the first two for free.

Least privilege is the right lesson — for **workload** roles, where the blast radius is a running container that an attacker might reach. You'll do that properly in [Module 7](07-secrets.md) and [Module 9](09-s3-and-cloudfront.md), where `upload-api` gets `PutObject` on `raw/*` and literally nothing else. That's where the scoping exercise teaches you something.

### Why `us-east-1`, non-negotiably

Three reasons, in priority order:

1. **ACM certificates used by CloudFront must be in us-east-1.** This is a hard AWS constraint with no exceptions and no workaround. CloudFront is a global service whose control plane lives in us-east-1, and it will only attach a certificate from that region. If your cluster were in `ca-central-1`, you would maintain *two* certificates for one domain — a regional one for the ALB and a us-east-1 one for CloudFront — with two DNS validation records and two independent renewal paths that fail at different times. Being in us-east-1 means one wildcard cert covers both. [Module 5](05-dns-and-certificates.md) issues it.
2. **It's the cheapest region** for EC2, EBS, RDS and NAT Gateway. On a budget where the EKS control plane alone is $73/month, a 5–10% regional delta is real money.
3. **Deepest spot capacity** for the compute-optimised instance families Karpenter will pick for VOD transcoding in [Module 6](06-karpenter.md). Thin spot pools mean frequent interruptions.

The counterpoint you should be able to state if someone asks: us-east-1 has the largest blast radius of any AWS region — when it has a bad day, a noticeable fraction of the internet has a bad day. And if Canadian data residency ever became a requirement, you'd move to `ca-central-1` at roughly +8% cost *and still* need a second us-east-1 certificate for CloudFront. This project stores stream keys and view timestamps, no personal data, so the cheap correct-by-construction option wins.

### The ARN gotcha, learned now rather than in Module 3

When you assume an Identity Center role, `aws sts get-caller-identity` returns an **assumed-role session ARN**:

```
arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_AdministratorAccess_abc1234567890def/intern
```

EKS Access Entries in [Module 3](03-eks-cluster.md) need the underlying **role ARN**, which looks different — it has the reserved-SSO path in it and no session name:

```
arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/us-east-1/AWSReservedSSO_AdministratorAccess_abc1234567890def
```

Write yours down at the end of this module. You will need it twice and it is annoying to reconstruct.

---

## Do it

### 1.1 — Enable Identity Center and create the user

This part is console-only and done once by whoever owns the AWS account. It cannot be scripted meaningfully from a laptop that has no credentials yet.

1. Sign in to the AWS console as the account root or an existing admin. **Set the region selector to `us-east-1` before you start** — Identity Center is regional and its home region cannot be changed later without deleting the instance.
2. **IAM Identity Center → Enable.** If the account is not part of an AWS Organization, this creates one with the account as management account. That's expected.
3. **Permission sets → Create permission set → Predefined → `AdministratorAccess`.** Set the session duration to 8 hours. (Default is 1 hour, which means logging in repeatedly during a 15-minute `eksctl create cluster`.)
4. **Users → Add user.** Username `intern`, a real email address — the invitation goes there and it's also the MFA enrolment path.
5. **AWS accounts → select the account → Assign users → `intern` → `AdministratorAccess`.**
6. **Settings → Authentication → Multi-factor authentication → Edit.** Set:
   - *Prompt users for MFA*: **Every time they sign in**
   - *If a user does not yet have a registered MFA device*: **Require them to register an MFA device at sign-in**

   Enforcing MFA is the control that makes the short-lived-credential design actually worth something. Without it, a stolen password is a full session.
7. Note the **AWS access portal URL** from the Identity Center dashboard. It looks like `https://d-1234567890.awsapps.com/start`. You need it in the next step.

The invited user completes the email invitation, sets a password, and enrols an MFA device (any TOTP app).

### 1.2 — Configure the CLI profile

Install the AWS CLI first if you haven't — see 1.5, or run `brew install awscli` now and come back for the version check.

```sh
aws configure sso
```

Answer as follows. The profile name is fixed by the [conventions table](README.md#conventions-used-throughout) and every later module assumes it:

```
SSO session name (Recommended): linkify
SSO start URL [None]: https://d-1234567890.awsapps.com/start
SSO region [None]: us-east-1
SSO registration scopes [sso:account:access]: <press enter>
```

A browser opens; approve the request. The CLI then lists the accounts and roles you have access to:

```
The only AWS account available to you is: 123456789012
Using the account ID 123456789012
The only role available to you is: AdministratorAccess
Using the role name "AdministratorAccess"
CLI default client Region [None]: us-east-1
CLI default output format [None]: json
CLI profile name [AdministratorAccess-123456789012]: linkify-streaming
```

Then set the two environment variables every shell block in this course assumes. Put them in your shell profile (`~/.zshrc` or `~/.bashrc`) so they survive a new terminal:

```sh
export AWS_PROFILE=linkify-streaming
export AWS_REGION=us-east-1
```

Log in:

```sh
aws sso login
```

You'll run that once a day. When a command fails with `The SSO session associated with this profile has expired or is otherwise invalid`, that's what it means.

### 1.3 — Cost Explorer, on day zero

```sh
# Console only — there is no CLI call that enables it.
# Billing and Cost Management → Cost Explorer → Launch Cost Explorer
```

Cost Explorer takes roughly **24 hours** to populate after you enable it, and it does not backfill from before you turned it on. Enabling it on day ten means you have no data for days one through ten — which are exactly the days you'll want to look at when you're trying to work out why the bill jumped.

While you're in the Billing console, activate cost allocation tags:

```sh
# Billing and Cost Management → Cost allocation tags → User-defined cost allocation tags
# Find "Project" → Activate
```

The tag will not appear in that list until at least one resource has been tagged with it, so you may have to come back after [Module 3](03-eks-cluster.md). Do come back. Without activated cost allocation tags, Cost Explorer cannot break the bill down by tag, and you will find yourself staring at a line called **"EC2-Other: $61"** with no way to learn that it's EBS volumes and NAT gateway data processing. Everything you create in this course carries `Project=streaming`.

### 1.4 — The budget alarm, before you create anything

```sh
aws budgets create-budget \
  --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --budget '{
    "BudgetName": "streaming-monthly",
    "BudgetLimit": {"Amount": "200", "Unit": "USD"},
    "TimeUnit": "MONTHLY",
    "BudgetType": "COST"
  }' \
  --notifications-with-subscribers '[
    {"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":50,"ThresholdType":"PERCENTAGE"},
     "Subscribers":[{"SubscriptionType":"EMAIL","Address":"info@linkifysolutions.com"}]},
    {"Notification":{"NotificationType":"FORECASTED","ComparisonOperator":"GREATER_THAN","Threshold":100,"ThresholdType":"PERCENTAGE"},
     "Subscribers":[{"SubscriptionType":"EMAIL","Address":"info@linkifysolutions.com"}]}
  ]'
```

Two notifications, and the difference between them matters:

- **ACTUAL at 50%** fires when you have already spent $100. It's a status report.
- **FORECASTED at 100%** fires when AWS's projection of your month-end spend crosses $200 — which can happen on day three. That's the one that saves you money, because it fires while there's still something you can do.

Budgets is a global service; the `aws budgets` API is only available in `us-east-1`, which is where you already are.

The $200 limit is set against the ~$240/month all-on figure from the [README](README.md#what-this-costs). It is meant to fire. Treat the first FORECASTED email as information, not an emergency — but read it.

### 1.5 — Tools, pinned

Version skew between `eksctl`, `kubectl` and the EKS API produces confusing errors, so pin these rather than taking whatever `brew` last cached.

| Tool | Version | Why this version |
|---|---|---|
| `aws` CLI | v2, ≥ 2.20 | v1 has no `configure sso`. Anything ≥2.20 is fine. |
| `kubectl` | 1.35.x | **Must be within one minor version of the cluster.** The cluster is 1.35 ([Module 3](03-eks-cluster.md)). |
| `eksctl` | ≥ 0.200 | Needs access-entry and pod-identity-association support in the cluster config schema. |
| `helm` | ≥ 3.16 | Every add-on from [Module 4](04-addons-and-storage.md) onward is a Helm chart. |
| `k9s` | latest, optional | A terminal UI for the cluster. Optional, but debugging pod scheduling in it is much faster than repeated `kubectl describe`. |

macOS:

```sh
brew install awscli eksctl helm k9s

# kubectl pinned to match the cluster. Apple Silicon shown; use amd64 on Intel.
curl -LO "https://dl.k8s.io/release/v1.35.7/bin/darwin/arm64/kubectl"
chmod +x kubectl && sudo mv kubectl /usr/local/bin/kubectl
```

Linux / WSL2:

```sh
curl -LO "https://dl.k8s.io/release/v1.35.7/bin/linux/amd64/kubectl"
chmod +x kubectl && sudo mv kubectl /usr/local/bin/kubectl
```

The `kubectl` **±1 minor** rule is the one to internalise. The Kubernetes project supports a client one minor version older or newer than the server. A 1.33 client against a 1.35 server may work for `get pods` and then fail on something subtler — an API version that doesn't exist yet, a field the client strips silently on `apply`. When a `kubectl` command behaves inexplicably later in this course, check `kubectl version` before you check anything else.

Confirm all four:

```sh
kubectl version --client && eksctl version && helm version --short && aws --version
```

---

## Verify

This is the checkpoint. It has to pass before [Module 2](02-vpc-and-networking.md).

```sh
aws sts get-caller-identity
```

```json
{
    "UserId": "AROAEXAMPLEID:intern",
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_AdministratorAccess_abc1234567890def/intern"
}
```

Three things to check in that output, not just that it succeeded:

1. **`Account`** is the account you expect. If you have access to more than one, this is where you find out you picked the wrong one.
2. **`Arn`** contains `assumed-role/AWSReservedSSO_`. If it says `user/something` instead, you are on an IAM user credential — probably an old `[default]` profile in `~/.aws/credentials` shadowing your intent. Check `echo $AWS_PROFILE`.
3. The **role suffix** — `AWSReservedSSO_AdministratorAccess_abc1234567890def` in the example. Save the full IAM role ARN now:

```sh
aws sts get-caller-identity --query Arn --output text
# arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_AdministratorAccess_abc1234567890def/intern

# The role ARN that EKS Access Entries need, derived from the above:
# arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/us-east-1/AWSReservedSSO_AdministratorAccess_abc1234567890def
```

You can also list it directly, which avoids transcription errors:

```sh
aws iam list-roles --path-prefix /aws-reserved/sso.amazonaws.com/ \
  --query 'Roles[?starts_with(RoleName, `AWSReservedSSO_AdministratorAccess`)].Arn' --output text
```

Then confirm the budget exists:

```sh
aws budgets describe-budgets \
  --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --query 'Budgets[].{Name:BudgetName,Limit:BudgetLimit.Amount}' --output table
```

```
-------------------------------------
|          DescribeBudgets          |
+--------+--------------------------+
|  Limit |          Name            |
+--------+--------------------------+
|  200.0 |  streaming-monthly       |
+--------+--------------------------+
```

And that the region is right, because a wrong region here silently produces a cluster you cannot put a CloudFront certificate on:

```sh
aws configure get region --profile linkify-streaming
```

```
us-east-1
```

---

## What breaks

**`Error loading SSO Token: Token for linkify does not exist` or `The SSO session associated with this profile has expired`.** Far and away the most common, because it recurs daily.

```sh
aws sso login
```

**Commands use the wrong identity — an old IAM user, or the wrong account.** `AWS_PROFILE` isn't set in this shell, or a `[default]` profile in `~/.aws/credentials` is taking precedence. Env vars beat config files, so:

```sh
echo $AWS_PROFILE
aws configure list        # shows which profile and where each value came from
```

The `Type` column in `aws configure list` tells you whether a value came from the environment, the config file, or a default — that's the fastest way to find a shadowing credential.

**The browser opens for `aws sso login` and the approval succeeds, but the CLI still fails.** Usually a mismatched SSO region: the session was registered against one region and the portal lives in another. Check `~/.aws/config`; the `[sso-session linkify]` block must have `sso_region = us-east-1`. If it doesn't, re-run `aws configure sso`.

**`aws budgets create-budget` returns `AccessDeniedException`.** Budget and billing APIs are gated separately from IAM permissions. In an Organization, the management account must have **IAM access to billing** enabled (Account settings → IAM user and role access to Billing Information). `AdministratorAccess` alone is not enough if that toggle is off.

**Cost Explorer shows no data.** It's been less than 24 hours. There is nothing to fix; this is why the module says to enable it on day zero.

**`kubectl` behaves strangely against the cluster in a later module.** Check the version skew first, before you debug anything else:

```sh
kubectl version
```

Client and server minor versions must be within one of each other. If you installed `kubectl` via `brew` rather than the pinned URL, you likely have whatever the latest release is, which may be two or more minors ahead of the 1.35 cluster.

---

Next: [Module 2 — The VPC](02-vpc-and-networking.md).
