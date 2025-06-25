.PHONY: serve
serve:
	./serve.sh

.PHONY: clean
clean:
	$(RM) -r dagster/{.logs_queue,.nux,.telemetry,history,schedules,storage,logs}
