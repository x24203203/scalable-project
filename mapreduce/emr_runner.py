#!/usr/bin/env python3
"""
Simple EMR Runner for Word Count MapReduce Benchmarks
Runs benchmarks with 3, 5, and 7 cores
"""

import boto3
import time
import json
from datetime import datetime

# Configuration
S3_BUCKET = "scalable-x24203203"
YELP_REVIEWS_PATH = "s3://scalable-x24203203/yelp_academic_dataset_review.json"
SCRIPT_S3_PATH = "s3://scalable-x24203203/scripts/word_count_mapreduce.py"

class WordCountBenchmarkRunner:
    def __init__(self):
        self.emr_client = boto3.client('emr', region_name='us-east-1')
        self.s3_client = boto3.client('s3')
        self.ec2_client = boto3.client('ec2')
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def upload_script(self, local_script_path="word_count_mapreduce.py"):
        """Upload the MapReduce script to S3"""
        print(f"Uploading script to S3...")
        try:
            self.s3_client.upload_file(
                local_script_path,
                S3_BUCKET,
                "scripts/word_count_mapreduce.py"
            )
            print(f"✓ Script uploaded to: {SCRIPT_S3_PATH}")
        except Exception as e:
            print(f"✗ Error uploading script: {e}")
            return False
        return True
    
    def get_subnet_id(self):
        """Get default VPC subnet ID"""
        try:
            vpcs = self.ec2_client.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
            vpc_id = vpcs['Vpcs'][0]['VpcId']
            subnets = self.ec2_client.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
            return subnets['Subnets'][0]['SubnetId']
        except Exception as e:
            print(f"Error getting subnet: {e}")
            return None
    
    def create_emr_cluster(self, num_cores):
        """Create an EMR cluster with specified number of core nodes"""
        cluster_name = f"WordCount-{num_cores}cores-{self.timestamp}"
        subnet_id = self.get_subnet_id()
        
        if not subnet_id:
            print("Failed to get subnet ID")
            return None
        
        print(f"\nCreating EMR cluster: {cluster_name}")
        
        try:
            response = self.emr_client.run_job_flow(
                Name=cluster_name,
                ReleaseLabel='emr-6.10.0',
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hadoop'}
                ],
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1
                        },
                        {
                            'Name': 'Worker',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': num_cores
                        }
                    ],
                    'Ec2SubnetId': subnet_id,
                    'KeepJobFlowAliveWhenNoSteps': False  # Auto-terminate
                },
                Steps=[
                    {
                        'Name': f'WordCount-{num_cores}cores',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': [
                                'spark-submit',
                                '--deploy-mode', 'client',
                                '--master', 'yarn',
                                '--conf', f'spark.executor.instances={num_cores * 2}',
                                '--conf', 'spark.executor.cores=2',
                                '--conf', 'spark.executor.memory=3g',
                                SCRIPT_S3_PATH,
                                '--input-path', YELP_REVIEWS_PATH,
                                '--output-bucket', S3_BUCKET,
                                '--num-cores', str(num_cores)
                            ]
                        }
                    }
                ],
                ServiceRole='EMR_DefaultRole',
                JobFlowRole='EMR_EC2_DefaultRole',
                VisibleToAllUsers=True,
                LogUri=f's3://{S3_BUCKET}/emr-logs/',
                StepConcurrencyLevel=1
            )
            
            cluster_id = response['JobFlowId']
            print(f"✓ Cluster created: {cluster_id}")
            return cluster_id
            
        except Exception as e:
            print(f"✗ Failed to create cluster: {e}")
            return None
    
    def monitor_cluster(self, cluster_id):
        """Monitor cluster execution"""
        print(f"Monitoring cluster {cluster_id}...")
        start_time = time.time()
        last_state = None
        
        while True:
            try:
                cluster = self.emr_client.describe_cluster(ClusterId=cluster_id)
                state = cluster['Cluster']['Status']['State']
                
                if state != last_state:
                    print(f"Cluster state: {state}")
                    last_state = state
                
                if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
                    elapsed = (time.time() - start_time) / 60
                    
                    if state == 'TERMINATED':
                        print(f"✓ Job completed successfully in {elapsed:.1f} minutes")
                        return True, elapsed
                    else:
                        print(f"✗ Job failed after {elapsed:.1f} minutes")
                        reason = cluster['Cluster']['Status'].get('StateChangeReason', {})
                        print(f"Failure reason: {reason.get('Message', 'Unknown')}")
                        return False, elapsed
                
                time.sleep(30)
                
            except Exception as e:
                print(f"Error monitoring cluster: {e}")
                return False, 0
    
    def run_benchmarks(self, core_configs=[5, 7]):
        """Run benchmarks with different core configurations"""
        print(f"\n{'=' * 60}")
        print(f"Starting Word Count MapReduce Benchmarks")
        print(f"Dataset: Yelp Reviews (~5GB)")
        print(f"Configurations: {core_configs} cores")
        print(f"{'=' * 60}")
        
        # Upload script
        if not self.upload_script():
            print("Failed to upload script")
            return
        
        results = {}
        
        for num_cores in core_configs:
            print(f"\n{'=' * 50}")
            print(f"Benchmark with {num_cores} cores")
            print(f"{'=' * 50}")
            
            cluster_id = self.create_emr_cluster(num_cores)
            if not cluster_id:
                results[num_cores] = {'success': False, 'error': 'Failed to create cluster'}
                continue
            
            success, elapsed_minutes = self.monitor_cluster(cluster_id)
            
            results[num_cores] = {
                'cluster_id': cluster_id,
                'success': success,
                'elapsed_minutes': elapsed_minutes
            }
        
        # Save summary
        self.save_summary(results)
        return results
    
    def save_summary(self, results):
        """Save benchmark summary"""
        summary = {
            'timestamp': self.timestamp,
            'dataset': 'Yelp Reviews',
            'operation': 'Word Count MapReduce',
            'results': results
        }
        
        # Save locally
        filename = f'wordcount_benchmark_summary_{self.timestamp}.json'
        with open(filename, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nLocal summary saved to: {filename}")
        
        # Save to S3
        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f'benchmarks/summary_{self.timestamp}.json',
            Body=json.dumps(summary, indent=2)
        )
        print(f"S3 summary saved to: s3://{S3_BUCKET}/benchmarks/summary_{self.timestamp}.json")
    
    def display_results(self, results):
        """Display benchmark results"""
        print(f"\n{'=' * 60}")
        print("BENCHMARK RESULTS SUMMARY")
        print(f"{'=' * 60}")
        
        print(f"\n{'Cores':<10} {'Status':<15} {'Time (min)':<15} {'Cluster ID':<20}")
        print("-" * 60)
        
        for cores, result in sorted(results.items()):
            status = 'Success' if result.get('success', False) else 'Failed'
            time_str = f"{result.get('elapsed_minutes', 0):.1f}" if result.get('success', False) else 'N/A'
            cluster_id = result.get('cluster_id', 'N/A')
            
            print(f"{cores:<10} {status:<15} {time_str:<15} {cluster_id:<20}")
        
        # Calculate speedup if all successful
        successful = [r for r in results.values() if r.get('success', False)]
        if len(successful) >= 2:
            baseline = results[3]['elapsed_minutes'] if 3 in results and results[3]['success'] else None
            if baseline:
                print(f"\n{'=' * 60}")
                print("PERFORMANCE ANALYSIS")
                print(f"{'=' * 60}")
                print(f"\n{'Cores':<10} {'Speedup':<15} {'Efficiency':<15}")
                print("-" * 40)
                
                for cores in sorted(results.keys()):
                    if results[cores]['success']:
                        speedup = baseline / results[cores]['elapsed_minutes']
                        efficiency = (speedup / (cores / 3)) * 100
                        print(f"{cores:<10} {speedup:<15.2f} {efficiency:<15.1f}%")


def main():
    """Main function"""
    runner = WordCountBenchmarkRunner()
    
    # Run benchmarks
    results = runner.run_benchmarks([3, 5, 7])
    
    # Display results
    runner.display_results(results)
    
    print("\n✓ Benchmarking complete!")
    print(f"\nCheck results in S3:")
    print(f"- Word counts: s3://{S3_BUCKET}/results/word_count/")
    print(f"- Benchmarks: s3://{S3_BUCKET}/benchmarks/")
    print(f"- Logs: s3://{S3_BUCKET}/emr-logs/")


if __name__ == "__main__":
    main()
