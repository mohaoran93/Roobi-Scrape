#!/bin/bash
# Hardcoded configuration - no overrides allowed
GEOLOC="25.042983969226388,55.23489620876715"
H3_CELLS="8a43a1220467fff"
INDEX_POSTFIX="20250506"
GEO_ALIAS="motor_city"
PRIORITY="50"
TEST_MODE="true"

# Index management flags
RECREATE_INDEXES="false" # Set to "true" to recreate all indexes
CLEAR_EXISTING="false"   # Set to "true" to clear existing data (ignored if RECREATE_INDEXES is true)

echo "Starting scraper pipeline..."
echo "Location: $GEOLOC"
echo "H3 Cells: $H3_CELLS"
echo "Index Postfix: $INDEX_POSTFIX"
echo "Geo Alias: $GEO_ALIAS"
echo "Priority: $PRIORITY"
echo "Test Mode: $TEST_MODE"
echo "Recreate Indexes: $RECREATE_INDEXES"
echo "Clear Existing: $CLEAR_EXISTING"
echo "Date: $(date)"

# Create logs directory
mkdir -p logs

echo "=== Starting platform scrapers ==="

# Function to check if process is still running
wait_for_process() {
    local pid=$1
    local name=$2
    if [ ! -z "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "Waiting for $name (PID: $pid) to complete..."
        wait $pid
        local exit_code=$?
        echo "$name completed with exit code: $exit_code"
        return $exit_code
    else
        echo "$name process not found or already completed"
        return 1
    fi
}

# Prepare command with appropriate flags
FLAGS=""
if [ "$RECREATE_INDEXES" = "true" ]; then
    FLAGS="--recreate"
elif [ "$CLEAR_EXISTING" = "true" ]; then
    FLAGS="--clear"
fi

echo "work on careemquick"
if [ -d "careem_quick" ]; then
    cd careem_quick
    echo "Starting careem_quick from $(pwd) with flags: $FLAGS" | tee careem_quick.log
    python -u -m careem_quick.main ${GEOLOC} ${FLAGS} >> careem_quick.log 2>&1 &
    CAREEM_PID=$!
    cd ..
else
    echo "careem_quick directory not found, skipping"
    CAREEM_PID=""
fi

echo "work on carrefour"
if [ -d "carrefour" ]; then
    cd carrefour
    echo "Starting carrefour from $(pwd) with flags: $FLAGS" | tee carrefour.log
    python -u -m carrefour.main ${GEOLOC} ${FLAGS} >> carrefour.log 2>&1 &
    CARREFOUR_PID=$!
    cd ..
else
    echo "carrefour directory not found, skipping"
    CARREFOUR_PID=""
fi

echo "work on instashop"
if [ -d "instashop" ]; then
    cd instashop
    if [ -f main_xbytes.py ]; then
        echo "Starting instashop from $(pwd) with flags: $FLAGS" | tee instashop.log
        python -u -m instashop.main_xbytes ${GEOLOC} ${FLAGS} >> instashop.log 2>&1 &
        INSTASHOP_PID=$!
    else
        echo "instashop main_xbytes.py not found, skipping" | tee instashop.log
        INSTASHOP_PID=""
    fi
    cd ..
else
    echo "instashop directory not found, skipping"
    INSTASHOP_PID=""
fi

echo "work on kibsons"
if [ -d "kibsons" ]; then
    cd kibsons
    if [ -f main.py ]; then
        echo "Starting kibsons from $(pwd) with flags: $FLAGS" | tee kibsons.log
        python -u -m kibsons.main ${FLAGS} >> kibsons.log 2>&1 &
        KIBSONS_PID=$!
    else
        echo "kibsons main.py not found, skipping" | tee kibsons.log
        KIBSONS_PID=""
    fi
    cd ..
else
    echo "kibsons directory not found, skipping"
    KIBSONS_PID=""
fi

echo "work on noon"
if [ -d "noon" ]; then
    cd noon
    if [ -f main.py ]; then
        echo "Starting noon from $(pwd) with flags: $FLAGS" | tee noon.log
        python -u -m noon.main ${FLAGS} >> noon.log 2>&1 &
        NOON_PID=$!
    else
        echo "noon main.py not found, skipping" | tee noon.log
        NOON_PID=""
    fi
    cd ..
else
    echo "noon directory not found, skipping"
    NOON_PID=""
fi

echo "work on nownow"
if [ -d "nownow" ]; then
    cd nownow
    if [ -f main.py ]; then
        echo "Starting nownow from $(pwd) with flags: $FLAGS" | tee nownow.log
        python -u -m nownow.main ${GEOLOC} ${FLAGS} >> nownow.log 2>&1 &
        NOWNOW_PID=$!
    else
        echo "nownow main.py not found, skipping" | tee nownow.log
        NOWNOW_PID=""
    fi
    cd ..
else
    echo "nownow directory not found, skipping"
    NOWNOW_PID=""
fi

echo "work on talabat_app (Store level version)"
if [ -d "talabat" ]; then
    cd talabat
    if [ -f main.py ]; then
        echo "Starting talabat from $(pwd) with flags: $FLAGS" | tee talabat.log
        python -u -m talabat.main ${GEOLOC} ${FLAGS} >> talabat.log 2>&1 &
        TALABAT_PID=$!
    else
        echo "talabat main.py not found, skipping" | tee talabat.log
        TALABAT_PID=""
    fi
    cd ..
else
    echo "talabat directory not found, skipping"
    TALABAT_PID=""
fi

echo "waiting for all scraping to complete..."
# echo "Started processes: careem_quick($CAREEM_PID) carrefour($CARREFOUR_PID) instashop($INSTASHOP_PID) kibsons($KIBSONS_PID) noon($NOON_PID) nownow($NOWNOW_PID) talabat($TALABAT_PID)"
date

# Wait for all background processes with better error handling
[ ! -z "$CAREEM_PID" ] && wait_for_process $CAREEM_PID "careem_quick"
[ ! -z "$CARREFOUR_PID" ] && wait_for_process $CARREFOUR_PID "carrefour"
[ ! -z "$INSTASHOP_PID" ] && wait_for_process $INSTASHOP_PID "instashop"
[ ! -z "$KIBSONS_PID" ] && wait_for_process $KIBSONS_PID "kibsons"
[ ! -z "$NOON_PID" ] && wait_for_process $NOON_PID "noon"
[ ! -z "$NOWNOW_PID" ] && wait_for_process $NOWNOW_PID "nownow"
[ ! -z "$TALABAT_PID" ] && wait_for_process $TALABAT_PID "talabat"

echo "all scraping done"
date

echo "=== Scraping Summary ==="
echo "Check individual logs:"
ls -la *.log 2>/dev/null || echo "No log files found"

echo "=== Log file contents ==="
for log_file in *.log; do
    if [ -f "$log_file" ]; then
        echo "=== Contents of $(basename $log_file) ==="
        tail -n 20 "$log_file"
        echo ""
    fi
done

echo "=== Pipeline completed ==="
echo "Configuration used:"
echo "  Location: $GEOLOC"
echo "  H3 Cells: $H3_CELLS"
echo "  Index Postfix: $INDEX_POSTFIX"
echo "  Geo Alias: $GEO_ALIAS"
echo "  Priority: $PRIORITY"
echo "  Test Mode: $TEST_MODE"
echo "  Recreate Indexes: $RECREATE_INDEXES"
echo "  Clear Existing: $CLEAR_EXISTING"

# Keep container alive for debugging
echo "=== Container will stay alive for debugging ==="
echo "Use 'docker exec -it <container_name> bash' to investigate"