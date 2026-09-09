Major components of C-VAL
	1. Node Discovery Optimization
		a. Get node list
		b. Filter the nodes based onstale/expired test results OR no test results available
		c. Prioritized ranked queue of nodes
		d. Batch job submission
	2. Validation tests
		a. DL unit test - live
		b. Single node IB loop back NCCL all reduce test with ibbw tracker tool - live
		c. Vast storage performance test -live
		d. DeepSeek MOE readiness test ( intra + inter node ) - backlog
		e. Cluster Fabric Validation test - backlog
		f. Straggler GPU detection test- backlog
	3. Cval cli interface
		a. Cli interface ( pre-agentic ) so we can build tools on top
		b. Cval live loop to opportunistically submit jobs as nodes get free
		c. Live job monitoring ( status , progress , node cool down lists , cordination between 3 layers - job pods, evaluator pod , controller)
	4. Database
		a. Job logs
		b. Test run records
		c. Raw test results
		d. Baselines + thresholds info tables
		e. Health Classifications tables
		f. Node Health Rank lists
	5. Evaluation Engine
		a. Identify unique environments ( docker image , pytorch , nccl , GPU model, iterations etc. )
		b. Build baselines and thresholds automatically for each env
		c. Update thresholds as we have more test results to increase confidence
		d. Classify test results metrics
		e. Classify validation test level , node level
		f. Rank nodes using peer comparison and score
	6. On-demand node health validation
		a. Quick way ( within 5 mins) to test the nodes and generate Health report.
		b. Tests all major components in the node ( cpu , gpu , storage, IB , nvlink )
	7. Automated Node Cordoning + User acceptance test
		a. Ability for MSR to cordon the node gracefully and automatically without breaking the Lambda framework
		b. Rate limiters + circuit breakers
		c. Clear system to understand why nodes are cordoned
		d. Monitor node remediation process end to end ( cordon-> drain -> ticket creation -> Diagnosis -> Fix -> validation -> GCR acceptance validation -> uncordon healthy node )
		e.
    8. Online Validation ( backlog item)