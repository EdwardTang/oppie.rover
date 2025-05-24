.PHONY: build run-nats clean test-basic test-recovery test-all report integration-tests perf-report

# 默认目标
all: build test-all

# 构建Docker容器
build:
	docker-compose build

# 启动NATS服务器
run-nats:
	docker-compose up -d nats

# 启动全部容器
run:
	docker-compose up -d

# 停止所有容器
stop:
	docker-compose down

# 清理
clean: stop
	rm -rf data
	rm -rf __pycache__
	rm -rf *.csv *.json

# 安装依赖
deps:
	pip install -r requirements.txt

# 基本的延迟和吞吐量测试
test-basic: run-nats
	python nats_bench.py --count 10000 --format json --output results/basic.json

# 故障恢复测试
test-recovery: run-nats
	python recovery_bench.py --count 5000 --format json --output results/recovery.json

# 运行所有测试
test-all: test-basic test-recovery

# 异步发布测试
test-async: run-nats
	python nats_bench.py --count 10000 --async --format json --output results/async.json

# 测试不同消息大小
test-sizes: run-nats
	python nats_bench.py --count 5000 --size 256 --format json --output results/size_256.json
	python nats_bench.py --count 5000 --size 1024 --format json --output results/size_1k.json
	python nats_bench.py --count 5000 --size 4096 --format json --output results/size_4k.json
	python nats_bench.py --count 1000 --size 16384 --format json --output results/size_16k.json

# 测试不同并发级别
test-concurrency: run-nats
	python nats_bench.py --count 5000 --publishers 1 --subscribers 1 --format json --output results/p1_s1.json
	python nats_bench.py --count 5000 --publishers 2 --subscribers 2 --format json --output results/p2_s2.json
	python nats_bench.py --count 5000 --publishers 5 --subscribers 1 --format json --output results/p5_s1.json
	python nats_bench.py --count 5000 --publishers 1 --subscribers 5 --format json --output results/p1_s5.json

# 生成测试报告
report:
	@echo "生成性能测试报告..."
	@echo "延迟和吞吐量测试结果:"
	@cat results/basic.json | python -m json.tool
	@echo "故障恢复测试结果:"
	@cat results/recovery.json | python -m json.tool

# 运行集成测试
integration-tests: run
	@echo "启动完整测试环境..."
	@echo "等待服务启动..."
	@sleep 5
	@echo "运行集成测试..."
	cd ../tests/integration && python test_nats_adapter.py
	@echo "集成测试完成"

# 运行集成测试（使用pytest）
integration-tests-pytest: run
	@echo "启动完整测试环境..."
	@echo "等待服务启动..."
	@sleep 5
	@echo "运行集成测试（pytest）..."
	cd ../tests/integration && pytest -m integration test_nats_adapter.py -v
	@echo "集成测试完成"

# 生成性能报告
perf-report:
	@echo "生成全面性能报告..."
	cd ../../tools && python generate_perf_report.py
	@echo "性能报告已生成到 docs/performance_report.md"

# 一键运行集成测试和生成报告
test-and-report: integration-tests perf-report 