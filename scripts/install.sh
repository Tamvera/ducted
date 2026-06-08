#!/usr/bin/env sh
set -e

DUCT_DIR="/var/lib/duct"
CONF_DIR="/etc/duct"
VENV_PATH="${DUCT_DIR}/venv"
DUCT_USER="duct"
SERVICE_FILE="/lib/systemd/system/duct.service"
SERVICE_URL="https://raw.githubusercontent.com/Tamvera/ducted/refs/heads/master/scripts/duct.service"

echo "Creating install directories"
sudo mkdir -p "$DUCT_DIR"
sudo mkdir -p "$CONF_DIR"

if [ ! -f "$CONF_DIR/duct.yml" ]; then
    sudo touch "$CONF_DIR/duct.yml"
fi

echo "Creating $DUCT_USER user"
if ! id "$DUCT_USER" >/dev/null 2>&1; then
    sudo useradd -r -U -d "$DUCT_DIR" -s /sbin/nologin "$DUCT_USER"
fi

sudo chown -R "$DUCT_USER:$DUCT_USER" "$CONF_DIR"
sudo chown -R "$DUCT_USER:$DUCT_USER" "$DUCT_DIR"

if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtualenv in $VENV_PATH"
    sudo -u "$DUCT_USER" python3 -m venv "$VENV_PATH"
fi

sudo -u "$DUCT_USER" "$VENV_PATH/bin/pip" install -U ducted

echo "Installing systemd service"
sudo curl -fsSL -o "$SERVICE_FILE" "$SERVICE_URL"

sudo systemctl daemon-reload
sudo systemctl enable duct.service

echo "Done. Start with: sudo systemctl start duct.service"
