
## 🌐 24/7 VPS Deployment (Ubuntu / Debian)

Follow this beginner-friendly, step-by-step guide to deploy and run your bot 24/7 on a Linux VPS using background `screen` sessions.

---

### Step 1: Update System & Install Required Packages
Update your server's package repository and install essential utilities (`git`, `python3`, `pip`, `venv`, and `screen`):

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install git python3 python3-pip python3-venv screen -y

```

---

### Step 2: Clone Repository & Navigate to Directory

Download the project code from GitHub and enter the project folder:

```bash
git clone [https://github.com/drsimon6/Phoenix-Trade-Multi-Wallet-Tracker-.git](https://github.com/drsimon6/Phoenix-Trade-Multi-Wallet-Tracker-.git)
cd Phoenix-Trade-Multi-Wallet-Tracker-

```

---

### Step 3: Create & Activate Virtual Environment

Set up an isolated Python environment to handle required dependencies:

```bash
# 1. Create a virtual environment named .venv
python3 -m venv .venv

# 2. Activate the virtual environment
source .venv/bin/activate

# 3. Install required dependencies
pip install -r requirements.txt

```

---

### Step 4: Create Configuration Files (Before Starting Screens)

Create and save your configuration file(s) before launching background processes.

#### 1. Primary Configuration (`config.py`):

```bash
nano config.py

```

* Paste your configuration code into the terminal editor.
* **How to save & exit in Nano:**
1. Press `Ctrl + O` (Write Out)
2. Press `Enter` (Confirm file name)
3. Press `Ctrl + X` (Exit editor)



#### 2. (Optional) High-Volume Configuration (`config_high_volume.py`):

If running a separate instance for high-volume wallets:

```bash
nano config_high_volume.py

```

* Paste your high-volume configuration code.
* Save and exit using `Ctrl + O` -> `Enter` -> `Ctrl + X`.

---

### Step 5: Launch Bot Instances in Background Screens

#### Instance 1: Normal Volume Tracker (`phoenix-normal`)

```bash
# 1. Create and enter a screen session named phoenix-normal
screen -S phoenix-normal

# 2. Activate the virtual environment inside the screen
source .venv/bin/activate

# 3. Launch the bot with default config
python3 main.py config.py

```

* **Detach Screen:** Press `Ctrl + A`, then press `D`. The bot will continue running safely in the background.

---

#### Instance 2: High-Volume Tracker (`phoenix-highvol`)

```bash
# 1. Create and enter a screen session named phoenix-highvol
screen -S phoenix-highvol

# 2. Activate the virtual environment inside the screen
source .venv/bin/activate

# 3. Launch the bot with high-volume config
python3 main.py config_high_volume.py

```

* **Detach Screen:** Press `Ctrl + A`, then press `D`.

---

### Step 6: Useful Screen Management Commands

| Action | Command |
| --- | --- |
| **List all active screens** | `screen -ls` |
| **Re-attach to normal instance** | `screen -r phoenix-normal` |
| **Re-attach to high-volume instance** | `screen -r phoenix-highvol` |
| **Detach from inside a screen** | Press `Ctrl + A` then press `D` |
| **Stop all running bots** | `pkill -f main.py` |
| **Kill a specific screen session** | `screen -XS phoenix-normal quit` |

```

```
