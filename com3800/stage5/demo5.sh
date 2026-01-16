#!/bin/bash
# Stage 5 Demo Script
# Author: Daniel Inoyatov
# Partner: Eitan Marovitz

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
GATEWAY_HTTP_PORT=8080
BASE_PEER_PORT=8010
NUM_PEERS=7
GATEWAY_ID=0
HEARTBEAT_INTERVAL=3000  # milliseconds (should match your gossip interval)
OUTPUT_LOG="output.log"

# Clean up any existing output log and pids file
rm -f "$OUTPUT_LOG"
rm -f pids.txt

# Function to log to both console and file
log() {
    echo -e "$@" | tee -a "$OUTPUT_LOG"
}

# Function to log section headers
log_section() {
    log "\n${BLUE}========================================${NC}"
    log "${BLUE}$1${NC}"
    log "${BLUE}========================================${NC}\n"
}

# Function to cleanup processes on exit
cleanup() {
    log_section "STEP 10: SHUTTING DOWN"
    if [ -f pids.txt ]; then
        while read pid; do
            if ps -p $pid > /dev/null 2>&1; then
                log "Killing process $pid"
                kill -9 $pid 2>/dev/null || true
            fi
        done < pids.txt
        rm -f pids.txt
    fi
    log "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT

#Step 1: Build and test
log_section "STEP 1: BUILDING AND TESTING"
log "Running mvn test..."
if mvn test 2>&1 | tee -a "$OUTPUT_LOG"; then
    log "${GREEN}Build and tests passed!${NC}"
else
    log "${RED}Build or tests failed!${NC}"
    exit 1
fi

# Step 2: Start the cluster
log_section "STEP 2: STARTING CLUSTER"

# Create logs directory
mkdir -p logs

# Start gateway
log "Starting Gateway Server (ID: $GATEWAY_ID)..."
java -cp target/classes edu.yu.cs.com3800.stage5.DemoCluster > logs/gateway.out 2>&1 &
GATEWAY_PID=$!
echo $GATEWAY_PID >> pids.txt
log "Gateway PID: $GATEWAY_PID"

sleep 2

# Start peer servers
for i in $(seq 1 $NUM_PEERS); do
    log "Starting Peer Server $i..."
    java -cp target/classes edu.yu.cs.com3800.stage5.DemoPeerServer $i > logs/peer${i}.out 2>&1 &
    PEER_PID=$!
    echo $PEER_PID >> pids.txt
    log "Peer $i PID: $PEER_PID"
    sleep 0.5
done

log "${GREEN}All servers started${NC}"

# Step 3: Wait for leader election
log_section "STEP 3: WAITING FOR LEADER ELECTION"

MAX_WAIT=60
WAIT_COUNT=0
HAS_LEADER=false

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))

    STATUS=$(curl -s http://localhost:$GATEWAY_HTTP_PORT/status 2>/dev/null || echo "NO_RESPONSE")

    # Check for valid response with a real leader (not NO_LEADER, not NO_RESPONSE, not LEADER:-1)
    if [[ "$STATUS" != "NO_RESPONSE" ]] && [[ "$STATUS" != "NO_LEADER" ]] && [[ "$STATUS" == *"LEADER:"* ]]; then
        # Extract leader ID and verify it's a valid positive number
        TEMP_LEADER=$(echo "$STATUS" | grep "^LEADER:" | cut -d: -f2 | tr -d '[:space:]')
        if [[ "$TEMP_LEADER" =~ ^[0-9]+$ ]] && [ "$TEMP_LEADER" -ge 0 ]; then
            HAS_LEADER=true
            break
        fi
    fi

    log "Waiting for leader election... ($WAIT_COUNT/$MAX_WAIT)"
done

if [ "$HAS_LEADER" = false ]; then
    log "${RED}Leader election timed out!${NC}"
    log "Last status response: $STATUS"
    log "Gateway log:"
    cat logs/gateway.out | tee -a "$OUTPUT_LOG"
    exit 1
fi

log "${GREEN}Leader elected!${NC}"
log "\nCluster Status (Server IDs and Roles):"
log "$STATUS"

# Extract leader ID for later use
LEADER_ID=$(echo "$STATUS" | grep "^LEADER:" | cut -d: -f2 | tr -d '[:space:]')
log "\nCurrent Leader ID: $LEADER_ID"

# Step 4: Send 9 client requests
log_section "STEP 4: SENDING 9 CLIENT REQUESTS"

for i in $(seq 1 9); do
    log "\n${YELLOW}--- Request $i ---${NC}"
    JAVA_CODE="public class TestClass$i {
    public String run() {
        return \"Result from request $i: \" + (${i} * ${i});
    }
}"

    log "Request $i Code:"
    log "$JAVA_CODE"
    
    RESPONSE=$(curl -s -X POST http://localhost:$GATEWAY_HTTP_PORT/compileandrun \
        -H "Content-Type: text/x-java-source" \
        -d "$JAVA_CODE" 2>&1)

    log "Response $i: $RESPONSE"
done

log "\n${GREEN}All 9 requests completed${NC}"

# Step 5: Kill a follower
log_section "STEP 5: KILLING A FOLLOWER"

# Refresh status to get current leader
STATUS=$(curl -s http://localhost:$GATEWAY_HTTP_PORT/status)
LEADER_ID=$(echo "$STATUS" | grep "^LEADER:" | cut -d: -f2 | tr -d '[:space:]')
log "Current Leader ID: $LEADER_ID"

# Pick a follower to kill (not the gateway, not the leader)
FOLLOWER_TO_KILL=""
for i in $(seq 1 $NUM_PEERS); do
    if [ "$i" != "$LEADER_ID" ]; then
        FOLLOWER_TO_KILL=$i
        break
    fi
done

log "${RED}Killing Follower $FOLLOWER_TO_KILL${NC}"

# Find the PID from pids.txt (line 1 = gateway, line 2 = peer 1, etc.)
FOLLOWER_LINE=$((FOLLOWER_TO_KILL + 1))
FOLLOWER_PID=$(sed -n "${FOLLOWER_LINE}p" pids.txt)

log "Follower $FOLLOWER_TO_KILL PID: $FOLLOWER_PID"
kill -9 $FOLLOWER_PID 2>/dev/null || true

# Wait heartbeat interval * 10 for failure detection
WAIT_TIME=$((HEARTBEAT_INTERVAL * 10 / 1000))
log "Waiting ${WAIT_TIME} seconds for failure detection..."
sleep $WAIT_TIME

log "\nUpdated Cluster Status (dead node should not appear):"
STATUS=$(curl -s http://localhost:$GATEWAY_HTTP_PORT/status)
log "$STATUS"

# Verify dead node is not in list
if echo "$STATUS" | grep -q "^${FOLLOWER_TO_KILL}:"; then
    log "${YELLOW}Warning: Dead follower $FOLLOWER_TO_KILL may still appear in list${NC}"
else
    log "${GREEN}Dead follower $FOLLOWER_TO_KILL correctly removed from list${NC}"
fi

# Step 6: Kill the leader and send 9 more requests
log_section "STEP 6: KILLING LEADER AND SENDING REQUESTS IN BACKGROUND"

# Refresh leader ID
STATUS=$(curl -s http://localhost:$GATEWAY_HTTP_PORT/status)
LEADER_ID=$(echo "$STATUS" | grep "^LEADER:" | cut -d: -f2 | tr -d '[:space:]')

log "${RED}Killing Leader $LEADER_ID${NC}"

# Find leader PID
LEADER_LINE=$((LEADER_ID + 1))
LEADER_PID=$(sed -n "${LEADER_LINE}p" pids.txt)

log "Leader $LEADER_ID PID: $LEADER_PID"
kill -9 $LEADER_PID 2>/dev/null || true

# Pause 1000 milliseconds as required
sleep 1

log "Sending 9 requests in background..."

# Send requests in background
for i in $(seq 10 18); do
    JAVA_CODE="public class TestClass$i {
    public String run() {
        return \"Background request $i result: \" + ($i * 2);
    }
}"

    (curl -s -X POST http://localhost:$GATEWAY_HTTP_PORT/compileandrun \
        -H "Content-Type: text/x-java-source" \
        -d "$JAVA_CODE" > /tmp/response_$i.txt 2>&1) &
done

log "Background requests submitted"

# Step 7: Wait for new leader
log_section "STEP 7: WAITING FOR NEW LEADER"

OLD_LEADER_ID=$LEADER_ID
MAX_WAIT=90
WAIT_COUNT=0
NEW_LEADER=false

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))

    STATUS=$(curl -s http://localhost:$GATEWAY_HTTP_PORT/status 2>/dev/null || echo "NO_RESPONSE")

    if [[ "$STATUS" != "NO_RESPONSE" ]] && [[ "$STATUS" != "NO_LEADER" ]] && [[ "$STATUS" == *"LEADER:"* ]]; then
        NEW_LEADER_ID=$(echo "$STATUS" | grep "^LEADER:" | cut -d: -f2 | tr -d '[:space:]')
        # Verify it's a valid positive number and different from old leader
        if [[ "$NEW_LEADER_ID" =~ ^[0-9]+$ ]] && [ "$NEW_LEADER_ID" != "$OLD_LEADER_ID" ] && [ -n "$NEW_LEADER_ID" ]; then
            NEW_LEADER=true
            break
        fi
    fi

    log "Waiting for new leader election... ($WAIT_COUNT/$MAX_WAIT)"
done

if [ "$NEW_LEADER" = true ]; then
    log "${GREEN}New leader elected!${NC}"
    log "New Leader ID: $NEW_LEADER_ID"
    log "\nNew Cluster Status:"
    log "$STATUS"
else
    log "${YELLOW}Warning: New leader not detected within timeout${NC}"
fi

# Wait only for the background request PIDs we launched (not all bg jobs)
log "\nWaiting for background requests to complete..."

# Assumes you populated BG_PIDS earlier when launching the background requests, e.g.:
#   BG_PIDS=()
#   curl ... > /tmp/response_10.txt & BG_PIDS+=("$!")
#   ...
for pid in "${BG_PIDS[@]}"; do
    wait "$pid"
done

log "\n${YELLOW}Background Request Responses:${NC}"
for i in $(seq 10 18); do
    log "\n--- Response $i ---"
    if [ -f /tmp/response_$i.txt ]; then
        RESPONSE=$(cat /tmp/response_$i.txt)
        log "$RESPONSE"
        rm /tmp/response_$i.txt
    else
        log "No response file found"
    fi
done


# Step 8: Send 1 more request in foreground
log_section "STEP 8: SENDING FINAL REQUEST (FOREGROUND)"

JAVA_CODE="public class FinalTest {
    public String run() {
        return \"Final request completed successfully!\";
    }
}"

log "${YELLOW}Final Request Code:${NC}"
log "$JAVA_CODE"

log "\n${YELLOW}Final Response:${NC}"
RESPONSE=$(curl -s -X POST http://localhost:$GATEWAY_HTTP_PORT/compileandrun \
    -H "Content-Type: text/x-java-source" \
    -d "$JAVA_CODE" 2>&1)
log "$RESPONSE"

# Step 9: List log files
log_section "STEP 9: LOG FILE PATHS"
log "Summary and Verbose Log Files for each node:"

# Get current status so we can extract real ports (avoid BASE_PEER_PORT guessing)
STATUS_NOW=$(curl -s "http://localhost:$GATEWAY_HTTP_PORT/status" 2>/dev/null || echo "")

# Search across all timestamped log dirs (more robust than only latest)
LOG_DIRS=(logs-*)
if [ "${LOG_DIRS[0]}" = "logs-*" ]; then
  log "No timestamped log directory found"
else
  log "\nLog directories: $(pwd)/${LOG_DIRS[*]}"

  for i in $(seq 0 $NUM_PEERS); do
    log "\n--- Server $i ---"

    # Try to extract the UDP port for this server from /status (first 4-5 digit number on the line)
    LINE=$(echo "$STATUS_NOW" | grep -E "^${i}:" || true)
    UDP_PORT=$(echo "$LINE" | grep -oE '[0-9]{4,5}' | head -1)

    # Fallback if status format doesn't include ports
    if [ -z "$UDP_PORT" ]; then
      UDP_PORT=$((BASE_PEER_PORT + i * 10))
    fi

    GOSSIP_PORT=$((UDP_PORT + 1))

    SUMMARY_LOG=$(find "${LOG_DIRS[@]}" -type f -iname "*summary*${GOSSIP_PORT}*log.txt" ! -iname "*.lck" 2>/dev/null | head -1)
    if [ -n "$SUMMARY_LOG" ]; then
      log "  Summary: $(pwd)/$SUMMARY_LOG"
    else
      log "  Summary: Not found (searched for gossip port $GOSSIP_PORT)"
    fi

    VERBOSE_LOG=$(find "${LOG_DIRS[@]}" -type f -iname "*verbose*${GOSSIP_PORT}*log.txt" ! -iname "*.lck" 2>/dev/null | head -1)
    if [ -n "$VERBOSE_LOG" ]; then
      log "  Verbose: $(pwd)/$VERBOSE_LOG"
    else
      log "  Verbose: Not found (searched for gossip port $GOSSIP_PORT)"
    fi
  done
fi

# Also list the stdout/stderr logs
log "\n--- Process Output Logs ---"
for i in $(seq 0 $NUM_PEERS); do
  if [ $i -eq 0 ]; then
    LOG_FILE="logs/gateway.out"
  else
    LOG_FILE="logs/peer${i}.out"
  fi

  if [ -f "$LOG_FILE" ]; then
    log "Server $i output: $(pwd)/$LOG_FILE"
  fi
done


# Step 10 is handled by the cleanup trap