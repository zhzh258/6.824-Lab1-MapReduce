pwd
rm -rf mr-[0-9]-[0-9]
rm -rf mr-out-*
echo "Building wc.go plugin..."
go build -race -buildmode=plugin ../mrapps/wc.go

echo "Starting the coordinator..."
(go run -race mrcoordinator.go pg*.txt 2>&1 | sed "s/^/[Coordinator] /") &

sleep 5

NUM_WORKERS=3
for(( i = 0; i < NUM_WORKERS; i++)); do
    echo "Starting worker $i..."
    (go run -race mrworker.go wc.so 2>&1 | sed "s/^/[Worker $i] /" ) &
done

wait
echo "All processes have completed"

rm -rf mr-[0-9]-[0-9]
