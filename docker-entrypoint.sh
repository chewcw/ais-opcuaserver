#!/bin/bash

# Create certs directory if it doesn't exist
mkdir -p /app/certs

# Check if certificates already exist
if [ ! -f /app/certs/cert.pem ] || [ ! -f /app/certs/key.pem ]; then
    echo "Generating OPC UA server certificates..."
    
    # Generate private key
    openssl genrsa -out /app/certs/key.pem 2048
    
    # Generate certificate request
    openssl req -new -key /app/certs/key.pem -out /app/certs/cert.csr -subj "/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN=pyopcua-server"
    
    # Generate self-signed certificate
    openssl x509 -req -days 365 -in /app/certs/cert.csr -signkey /app/certs/key.pem -out /app/certs/cert.pem
    
    # Set permissions
    chmod 600 /app/certs/key.pem
    chmod 644 /app/certs/cert.pem
    
    echo "Certificate generation complete."
else
    echo "Using existing certificates."
fi

# Function to forward signals to child processes
forward_signal() {
    kill -$1 $child_pid 2>/dev/null
}

# Setup signal handlers
trap 'forward_signal TERM; exit 0' TERM
trap 'forward_signal INT; exit 0' INT
trap 'forward_signal HUP; exit 0' HUP

# Start the Python application in the background
"$@" &
child_pid=$!

# Wait for the Python process to complete
wait $child_pid
exit_code=$?

# Exit with the same code as the child process
exit $exit_code

