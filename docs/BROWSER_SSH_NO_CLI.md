# Edit `.env` on the VM (no SSH app, no terminal on your Mac)

**Nobody outside your Google account can log into your VM for you**—not Cursor, not this chat. You can still do everything **in the browser** using Google’s own button (no `ssh` command to learn).

## 1. Open a terminal *inside* Google Cloud

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. Pick your project (e.g. `ultra-tendril-490700-q3`).
3. **☰ menu** → **Compute Engine** → **VM instances**.
4. Find your VM (e.g. `utility-tariff-finder`).
5. In the **Connect** column, click **SSH**.

A new browser window opens with a **black terminal** — you’re already logged in as your Linux user on the VM. No keys, no `ssh` from your laptop.

## 2. Go to the project folder

In that terminal, run (adjust the folder name if yours is different):

```bash
cd ~/utility-tariff-finder
```

If you’re not sure where it is:

```bash
ls
```

or:

```bash
find ~ -maxdepth 3 -name "docker-compose.yml" 2>/dev/null
```

## 3. Edit `.env`

```bash
nano .env
```

- Use the **arrow keys** to move.
- Add or change lines at the bottom (OAuth, `CORS_ORIGINS`, `PUBLIC_APP_URL`, etc.). Use the template in `deploy/oauth-nipio.env.snippet` on your Mac, or type the variables by hand.
- **Save:** press **Ctrl+O**, then **Enter**.
- **Exit:** press **Ctrl+X**.

If `nano` says the file doesn’t exist, create it first:

```bash
cp .env.docker.example .env
nano .env
```

## 4. Restart the stack

Still in the same browser terminal:

```bash
docker compose up -d --build
```

Wait until it finishes. Then open your app at your real URL (e.g. **`http://34.63.25.32.nip.io`** if you use nip.io).

## If the SSH button fails

- Use a **different browser** or allow pop-ups for `cloud.google.com`.
- In **VPC network → Firewall**, ensure **tcp:22** is allowed from your IP **or** use the Console SSH button only from a network that isn’t blocking outbound SSH (rare).

## Copy a file from your Mac without classic SSH

If you prefer to build `.env` on your Mac and upload it once:

On your **Mac** Terminal (replace user/IP/path):

```bash
scp "/path/to/Utility Tariff Finder/.env" YOUR_LINUX_USERNAME@34.63.25.32:~/utility-tariff-finder/.env
```

Your **Linux username** on the VM is often your Google username without `@...` (the SSH window title bar usually shows `you@utility-tariff-finder`). If `scp` asks for a password and fails, use the **browser SSH** + `nano` method instead.
