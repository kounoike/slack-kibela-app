from aws_cdk import core
import aws_cdk.aws_lambda as lambda_
from aws_cdk.aws_lambda_python import PythonFunction
import aws_cdk.aws_logs as logs
import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_secretsmanager as secretsmanager
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_logs as logs
import os.path as path
import json


class CdkStack(core.Stack):
    def create_enumerate_statemachine(self):
        enumerate_job = tasks.LambdaInvoke(
            self,
            "Enumerate Notes Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object({"action": "enumerate_notes"}),
        )
        get_tf_job = tasks.LambdaInvoke(
            self,
            "Get Text Frequency Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tf",
                    "id.$": "$.id",
                    "contentUpdatedAt.$": "$.contentUpdatedAt",
                    "isArchived.$": "$.isArchived",
                }
            ),
        )
        map_job = sfn.Map(
            self, "Notes Map", items_path="$.Payload.id_list", max_concurrency=3
        )
        get_idf_job = tasks.LambdaInvoke(
            self,
            "Get Inter Document Frequency Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {"action": "update_idf", "notes.$": "$.Payload"}
            ),
        )
        map_tfidf_job = sfn.Map(
            self, "TF*IDF Notes Map", items_path="$", max_concurrency=20
        )
        get_tfidf_job = tasks.LambdaInvoke(
            self,
            "Get TF*IDF WordCloud Image Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tfidf_png",
                    "id.$": "$.Payload.notes.id",
                    "contentUpdatedAt.$": "$.Payload.notes.contentUpdatedAt",
                    "isArchived.$": "$.Payload.notes.isArchived",
                }
            ),
        )

        definition = enumerate_job.next(
            map_job.iterator(get_tf_job.next(get_idf_job))
        ).next(map_tfidf_job.iterator(get_tfidf_job))
        self.enumerate_statemachine = sfn.StateMachine(
            self,
            "EnumerateStateMachine",
            definition=definition,
            timeout=core.Duration.minutes(10),
        )

    def create_update_statemachine(self):
        # update Workflow
        get_note_job = tasks.LambdaInvoke(
            self,
            "Get Note Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {"action": "get_note_from_url", "url.$": "$.url"}
            ),
        )
        get_tf_job_update = tasks.LambdaInvoke(
            self,
            "Get Text Frequency Job for Update",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tf",
                    "id.$": "$.Payload.id",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )
        get_idf_job_update = tasks.LambdaInvoke(
            self,
            "Get Inter Document Frequency Job for Update",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_idf",
                    "id.$": "$.Payload.id",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )
        get_tfidf_job_update = tasks.LambdaInvoke(
            self,
            "Get TF*IDF WordCloud Image Job for Update",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tfidf_png",
                    "id.$": "$.Payload.id",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )

        update_note_definition = get_note_job.next(
            get_tf_job_update.next(get_idf_job_update.next(get_tfidf_job_update))
        )

        self.update_note_statemachine = sfn.StateMachine(
            self,
            "UpdateNoteStateMachine",
            definition=update_note_definition,
            timeout=core.Duration.minutes(10),
        )

    def create_unfurl_statemachine(self):
        map_job = sfn.Map(self, "Unfurl Map", items_path="$.links", max_concurrency=10)
        get_note_job = tasks.LambdaInvoke(
            self,
            "Get Note Job for Unfurl",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {"action": "get_note_from_url", "url.$": "$.url"}
            ),
        )
        get_tf_job = tasks.LambdaInvoke(
            self,
            "Get Text Frequency Job for Unfurl",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tf",
                    "id.$": "$.Payload.id",
                    "url.$": "$.Payload.url",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )
        get_idf_job = tasks.LambdaInvoke(
            self,
            "Get Inter Document Frequency Job for Unfurl",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_idf",
                    "id.$": "$.Payload.id",
                    "url.$": "$.Payload.url",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )
        get_tfidf_job = tasks.LambdaInvoke(
            self,
            "Get TF*IDF WordCloud Image Job for Unfurl",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "update_tfidf_png",
                    "id.$": "$.Payload.id",
                    "url.$": "$.Payload.url",
                    "contentUpdatedAt.$": "$.Payload.contentUpdatedAt",
                    "isArchived.$": "$.Payload.isArchived",
                }
            ),
        )
        unfurl_job = tasks.LambdaInvoke(
            self,
            "Get Attachment Job",
            lambda_function=self.step_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "unfurl",
                    "id.$": "$.Payload.id",
                    "url.$": "$.Payload.url",
                }
            ),
        )

        get_tf_job.next(get_idf_job.next(get_tfidf_job.next(unfurl_job)))

        choice_job = sfn.Choice(self, "Check for Update")
        choice_job.when(
            sfn.Condition.and_(
                sfn.Condition.is_timestamp("$.Payload.tfidfPngUpdatedAt"),
                sfn.Condition.timestamp_less_than_json_path(
                    "$.Payload.contentUpdatedAt", "$.Payload.tfidfPngUpdatedAt"
                ),
            ),
            unfurl_job,
        ).when(
            sfn.Condition.and_(
                sfn.Condition.is_timestamp("$.Payload.tfTsvUpdatedAt"),
                sfn.Condition.timestamp_less_than_json_path(
                    "$.Payload.contentUpdatedAt", "$.Payload.tfTsvUpdatedAt"
                ),
            ),
            get_tfidf_job,
        ).otherwise(
            get_tf_job
        )

        unfurl_definition = map_job.iterator(get_note_job.next(choice_job))
        self.unfurl_statemachine = sfn.StateMachine(
            self,
            "UnfurlStateMachine",
            definition=unfurl_definition,
            timeout=core.Duration.minutes(20),
            state_machine_type=sfn.StateMachineType.EXPRESS,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "UnfurlStateMachineLogGroup"),
                level=sfn.LogLevel.ERROR,
            ),
        )

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # self.secret = secretsmanager.Secret.from_secret_name_v2(self, "BoltSecret", "prod/BoltLambdaContainerTest")
        # self.secret = secretsmanager.Secret(
        #     self,
        #     "AppSecret",
        #     secret_name="SlackKibelaAppSecret",
        #     generate_secret_string=secretsmanager.SecretStringGenerator(
        #         generate_string_key="dummy",
        #         secret_string_template=json.dumps({
        #             "SLACK_SIGNING_SECRET": "****",
        #             "SLACK_BOT_TOKEN": "xoxb-***"
        #         }),
        #     ),
        # )
        self.ssm_signing_secret = ssm.StringParameter(
            self, "SLACK_SIGNING_SECRET", string_value="xxxx"
        )
        self.ssm_bot_token = ssm.StringParameter(
            self, "SLACK_BOT_TOKEN", string_value="xoxb-xxxx"
        )
        self.ssm_kibela_team = ssm.StringParameter(
            self, "KIBELA_TEAM", string_value="teamname"
        )
        self.ssm_kibela_token = ssm.StringParameter(
            self, "KIBELA_TOKEN", string_value="secret/xxxx"
        )

        self.public_s3 = s3.Bucket(self, "PublicS3Bucket", public_read_access=True)

        self.private_s3 = s3.Bucket(self, "PrivateS3Bucket")

        self.step_lambda = lambda_.DockerImageFunction(
            self,
            "StepFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                path.join(path.dirname(__name__), "../wordcloud-app"),
                cmd=["app.handler"],
                entrypoint=["/usr/local/bin/python", "-m", "awslambdaric"],
            ),
            environment={
                "SSM_KIBELA_TEAM": self.ssm_kibela_team.parameter_name,
                "SSM_KIBELA_TOKEN": self.ssm_kibela_token.parameter_name,
                "S3_PUBLIC": self.public_s3.bucket_name,
                "S3_PRIVATE": self.private_s3.bucket_name,
            },
            log_retention=logs.RetentionDays.FIVE_DAYS,
            timeout=core.Duration.seconds(600),
            memory_size=2048,
        )

        self.create_enumerate_statemachine()
        self.create_update_statemachine()
        self.create_unfurl_statemachine()

        self.bolt_lambda = PythonFunction(
            self,
            "BoltFunction",
            entry=path.join(path.dirname(__name__), "../bolt-app/app"),
            index="app.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={
                # "SLACK_SIGNING_SECRET": self.secret.secret_value_from_json(
                #     "SLACK_SIGNING_SECRET"
                # ).to_string(),
                # "SLACK_BOT_TOKEN": self.secret.secret_value_from_json(
                #     "SLACK_BOT_TOKEN"
                # ).to_string(),
                "SSM_SLACK_SIGNING_SECRET": self.ssm_signing_secret.parameter_name,
                "SSM_SLACK_BOT_TOKEN": self.ssm_bot_token.parameter_name,
                "UPDATE_STATEMACHINE_ARN": self.update_note_statemachine.state_machine_arn,
                "UNFURL_STATEMACHINE_ARN": self.unfurl_statemachine.state_machine_arn,
            },
            log_retention=logs.RetentionDays.FIVE_DAYS,
            timeout=core.Duration.seconds(600),
        )
        self.apigw = apigateway.LambdaRestApi(
            self, "BoltRestGw", handler=self.bolt_lambda
        )

        self.ssm_kibela_team.grant_read(self.step_lambda)
        self.ssm_kibela_token.grant_read(self.step_lambda)
        self.public_s3.grant_read_write(self.step_lambda)
        self.public_s3.grant_delete(self.step_lambda)
        self.private_s3.grant_read_write(self.step_lambda)
        self.private_s3.grant_delete(self.step_lambda)

        self.bolt_lambda.add_to_role_policy(
            iam.PolicyStatement(resources=["*"], actions=["lambda:InvokeFunction"])
        )
        self.ssm_signing_secret.grant_read(self.bolt_lambda)
        self.ssm_bot_token.grant_read(self.bolt_lambda)

        self.update_note_statemachine.grant_start_execution(self.bolt_lambda)
        self.unfurl_statemachine.grant(self.bolt_lambda, "states:StartSyncExecution")

        # Rule(self, "KeepWarmEvents",
        # schedule=Schedule.expression("rate(5 minutes)"),
        # targets=[LambdaFunction(self.lambda_)])
