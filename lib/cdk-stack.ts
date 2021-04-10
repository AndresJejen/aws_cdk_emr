import iam = require('@aws-cdk/aws-iam');
import s3 = require('@aws-cdk/aws-s3');
import ec2 = require('@aws-cdk/aws-ec2');
import sfn = require('@aws-cdk/aws-stepfunctions');
import sfn_tasks = require('@aws-cdk/aws-stepfunctions-tasks');
import cdk = require('@aws-cdk/core');

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'CustomVPC', {
      cidr: '10.0.0.0/16',
      maxAzs: 2,
      subnetConfiguration: [{
          cidrMask: 26,
          name: 'publicSubnet',
          subnetType: ec2.SubnetType.PUBLIC,
      }],
      natGateways: 0
    });

    const bucket_name = process.env.bucket_name || "";
    const account_id  = process.env.account_id; 
    const bucket_data_arn = process.env.data_bucket_arn || "";

    const scripts_bucket = new s3.Bucket(this, bucket_name, {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      bucketName: bucket_name
    });

    const EMR_Applications: sfn_tasks.EmrCreateCluster.ApplicationConfigProperty[] = [
      {
        name: "Spark"
      },
      {
        name: "Livy"
      }
    ];

    const emr_ec2_role = iam.Role.fromRoleArn(
      this,
      'EMR_EC2_DefaultRole',
      `arn:aws:iam::${account_id}:role/EMR_EC2_DefaultRole`
    );

    const emr_role = iam.Role.fromRoleArn(
      this,
      'EMR_DefaultRole',
      `arn:aws:iam::${account_id}:role/EMR_DefaultRole`
    );

    emr_role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
    );

    const EMRActionsPolicy = new iam.ManagedPolicy(this, "EMRFullAccess", {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            "elasticmapreduce:CancelSteps",
            "elasticmapreduce:CancelSteps",
            "elasticmapreduce:TerminateJobFlows",
            "elasticmapreduce:DescribeCluster",
            "elasticmapreduce:AddJobFlowSteps",
            "elasticmapreduce:RunJobFlow"
          ],
          resources: ["arn:aws:elasticmapreduce:*:*:cluster/*"]
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            "events:PutTargets",
            "events:PutRule",
            "events:DescribeRule"
          ],
          resources: [`arn:aws:events:us-east-1:${account_id}:rule/StepFunctionsGetEventForEMRTerminateJobFlowsRule`]
        }),
        new iam.PolicyStatement(
          {
            effect: iam.Effect.ALLOW,
            actions: [
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets",
                "logs:CreateLogDelivery",
                "logs:GetLogDelivery",
                "logs:UpdateLogDelivery",
                "logs:DeleteLogDelivery",
                "logs:ListLogDeliveries",
                "logs:PutResourcePolicy",
                "logs:DescribeResourcePolicies",
                "logs:DescribeLogGroups",
            ],
            resources: [
                "*"
            ] 
        }),
        new iam.PolicyStatement(
          {
            effect: iam.Effect.ALLOW,
            actions: ["iam:PassRole"],
            resources: [
              emr_ec2_role.roleArn,
              emr_role.roleArn
            ]  
        }),
        new iam.PolicyStatement(
          {
            effect: iam.Effect.ALLOW,
            actions: [
              "s3:*"
            ],
            resources: [
              bucket_data_arn,
              scripts_bucket.bucketArn
            ]
        })
      ],
    });

    const step_function_role = new iam.Role(this, 'emr_role', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      managedPolicies: [
        //iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
        EMRActionsPolicy,
      ]
    });

    const emr_instances: sfn_tasks.EmrCreateCluster.InstancesConfigProperty = {
      ec2SubnetId: vpc.selectSubnets({subnetType: ec2.SubnetType.PUBLIC}).subnetIds[0],
      instanceFleets: [
        {
          instanceFleetType: sfn_tasks.EmrCreateCluster.InstanceRoleType.MASTER,
          targetOnDemandCapacity: 1,
          targetSpotCapacity: 0,
          instanceTypeConfigs: [
            {
              instanceType: "c4.large"
            }
          ]
        },
        {
          instanceFleetType: sfn_tasks.EmrCreateCluster.InstanceRoleType.CORE,
          targetOnDemandCapacity: 1,
          targetSpotCapacity: 0,
          instanceTypeConfigs: [
            {
              instanceType: "c4.large"
            }
          ]
        }
      ]
    };

    const create_cluster = new sfn_tasks.EmrCreateCluster(this,'Create_EMR_Cluster', {
      resultPath: '$.CreateClusterResult',
      name: "emr_cluster_cdk_demo",
      logUri: `s3://${scripts_bucket.bucketName}/cluster/logs/`,
      releaseLabel: "emr-6.2.0",
      applications: EMR_Applications,
      serviceRole: emr_role,
      clusterRole: emr_ec2_role,
      instances: emr_instances,
      ebsRootVolumeSize: cdk.Size.gibibytes(10),
      configurations: [
        {
          classification: "spark",
          properties: {
            "maximizeResourceAllocation": "true",    
          }
        },
        {
          classification: "spark-hive-site",
          properties: {
              "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
          }
        },
        {
          classification: "hive-site",
          properties: {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
          }
        },
        {
          classification: "presto-connector-hive",
          properties: {
            "hive.metastore.glue.datacatalog.enabled": "true"
          }
        },
        {
          classification: "spark-env",
          properties: {},
          configurations: [
            {
              classification: "export",
              properties: {
                "PYSPARK_PYTHON": "/usr/bin/python3"
              },
              configurations: []
            }
          ]
        }
      ],
      visibleToAllUsers: true,
    });

    const MergeResults = new sfn.Pass(
      this,
      'MergeResults',
      {
        parameters: {
          "ClusterId.$": "$.CreateClusterResult.ClusterId"
        }
      }
    );

    const cluster_termination_protection = new sfn_tasks.EmrSetClusterTerminationProtection(
      this,
      'Cluster_termination_protection',
      {
        resultPath: "$.ClusterTerminationResult",
        clusterId: sfn.TaskInput.fromDataAt('$.ClusterId').value,
        terminationProtected: true
      }
    );

    const add_step = new sfn_tasks.EmrAddStep(
      this,
      'Add_step_to_cluster',
      {
        clusterId: sfn.TaskInput.fromDataAt('$.ClusterId').value,
        integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        actionOnFailure: sfn_tasks.ActionOnFailure.CONTINUE,
        name: "Step_spark",
        jar: "command-runner.jar",
        args: [
            "spark-submit",
            `s3://${bucket_name}/getting_started.py`,
            "--bucket",
            bucket_name,
            "--data_uri",
            "epa_hap_daily_summary.csv",
        ],
        resultPath: '$.StepResult'
      }
    );

    const set_unprotected_termination_cluster = new sfn_tasks.EmrSetClusterTerminationProtection(
      this,
      'set_unprotected_termination',
      {
        clusterId: sfn.TaskInput.fromDataAt('$.ClusterId').value,
        terminationProtected: false,
        resultPath: "$.Unprotect_cluster_result"
      }
    );

    const terminate_cluster = new sfn_tasks.EmrTerminateCluster(
      this,
      'terminate_cluster',
      {
        clusterId: sfn.TaskInput.fromDataAt('$.ClusterId').value,
        integrationPattern: sfn.IntegrationPattern.RUN_JOB
      }
    );

    const chain = sfn.Chain
      .start(create_cluster)
      .next(MergeResults)
      .next(cluster_termination_protection)
      .next(add_step)
      .next(set_unprotected_termination_cluster)
      .next(terminate_cluster);

    new sfn.StateMachine(this, 'StateMachine', {
      definition: chain,
      stateMachineName: "ServerlessRunSteponEMR",
      role: step_function_role
    });

  }
}