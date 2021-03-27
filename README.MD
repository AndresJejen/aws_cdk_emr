# Data Infraestructure on AWS with AWS CDK (Cloud development kit)

In this repo you can find a CDK stak that builds a serverless application that runs AWS EMR when is needed.

## What is AWS CDK

Is a AWS Service that allows to create Infrastructure as code using the most popular languages like (Python, Typescript, Java and C#). You can deploy a simple stack to a complex modular infrastructure, edit it and make your cloud application scalable.   
Also cam be used to deploy data infrastructure, and maintain it in asingle project, no more fitting roles and permissions in the console UI. As simple as write a line of code you can add or revoke access to your infrastructure.

## What is this Repo About?

We will build a Apache Spark as a service application, the idea is user AWS EMR as a service. We only load a script in AWS S3 and we trigger a process that creates the cluster, run the job and destroy the cluster at end.

## Used Services in this project

- AWS CDK
- AWS Cloud Formation
- AWS S3
- AWS EMR
- AWS Lambda

## Getting started
1. Set environmentVariables
    ```
        export account_id=<Your Account Id>
        export bucket_name=apache-spark-latam-emr-cdk
    ```
2. Install node js
3. Install aws cdk
    ```
        npm install -g aws-cdk
    ```
4. Set your credentials user
    In this step you need a aws account and aws cli v2 installed in your computer
    ```
        aws configure --profile cdk
    ```
5. start your AWS cdk project
    ```
        cdk init app --language typescript
    ```
6. Write your code infrastructure
7. transpile and synthesize the code (if you're using typescript)
    ```
        npm run build
        cdk synth
    ```
8. Bootstrap
    ```
        cdk bootstrap --profile cdk
    ```
9. Deploy
    ```
        cdk deploy --profile cdk
    ```
10. Load your script
11. Wait for AWS makes magic and drink a coffee
12. Watch the result

## Data in this example

 - hazardous-air-pollutants you can find it in https://www.kaggle.com/epa/hazardous-air-pollutants


## Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template

### Who do I talk to? ###

* Repo owner or admin: Andres Jejen
* On twitter  @andres_jejen
* On LinkedIn https://www.linkedin.com/in/german-andres-jejen-cortes/

### Resources

- [BitBucket to S3](https://support.atlassian.com/bitbucket-cloud/docs/deploy-to-aws-with-s3/)
- [Typescript Documentation](https://docs.aws.amazon.com/cdk/api/latest/)
- [EMR configurations 2](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html)
- [EMR configuration classifications](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-6x.html#emr-620-class)
- [EMR spark configurration](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html)
- [AWS Data Wrangler, view code to adapt it](https://github.com/awslabs/aws-data-wrangler)
- [Custom VPC AWS CDK](https://blog.codecentric.de/en/2019/09/aws-cdk-create-custom-vpc/)
- [AWS CDK STEP FUNCTIONS](https://sbstjn.com/blog/aws-cdk-state-machine-step-functions-lambda/)
- [AWS CDK Step Functions AWS EMR](https://www.gitmemory.com/issue/aws/aws-cdk/5224/564808235)  