import subprocess

import boto3
import sagemaker


def get_execution_role(sagemaker_session=None):
    """
    Returns the role ARN whose credentials are used to call the API.
    In AWS notebook instance, this will return the ARN attributed to the
    notebook. Otherwise, it will return the ARN for the newest AmazonSageMaker-role.
    :param: sagemaker_session(Session): Current sagemaker session
    :rtype: string: the role ARN
    """
    try:
        role = sagemaker.get_execution_role(sagemaker_session=sagemaker_session)
    except ValueError as e:
        # When running locally we don't have sagemaker execution role available, and need to figure it out
        iam = boto3.resource('iam')
        iam_client = boto3.client('iam')
        service_roles = iam_client.list_roles(PathPrefix='/service-role/')['Roles']
        # Autocreated sagemaker rolenames are prefixed with AmazonSageMaker
        sagemaker_roles = list(filter(lambda x: x['RoleName'].startswith('AmazonSageMaker'), service_roles))
        # Should probably use 'CreateDate' for sorting instead, but generated RoleNames have a datetimestamp appended,
        # so sorting these in reverse order gives newest role at beginning of list
        sagemaker_roles.sort(key=lambda x: x['RoleName'], reverse=True)
        newest_sagemaker_rolename = sagemaker_roles[0]['RoleName']
        role = iam.Role(newest_sagemaker_rolename)
    return role


def create_model(training_job_name: str):
    """
    Creating SageMaker model from training_job_name with SageMaker low-level API.
    :param training_job_name:
    :return: Name of created (or existing) model.
    """
    sm_client = boto3.client('sagemaker')
    model_name = training_job_name + '-model'
    print(model_name)
    training_info = sm_client.describe_training_job(TrainingJobName=training_job_name)
    model_data = training_info['ModelArtifacts']['S3ModelArtifacts']
    training_image = training_info['AlgorithmSpecification']['TrainingImage']
    role = training_info['RoleArn']
    print(model_data)
    primary_container = {
        'Image': training_image,
        'ModelDataUrl': model_data
    }
    try:
        create_model_response = sm_client.create_model(
            ModelName=model_name,
            ExecutionRoleArn=role,
            PrimaryContainer=primary_container)

        print(create_model_response['ModelArn'])
        return model_name

    except:
        print(f'Model <{model_name}> already exists, continuing...')
        return model_name


def batch_transform(model_name: str, input_file: str, output_location: str):
    """
    Initialize and start a batch transform job on SageMaker from given model name (output from create_model)
    :param model_name: Model name to be used in batch transform
    :param input_file: s3 uri path to csv file
    :param output_location: s3 uri path to output folder
    :return: output_location
    """
    # Initialize the transformer object
    transformer = sagemaker.transformer.Transformer(
        base_transform_job_name='Batch-Transform',
        model_name=model_name,
        assemble_with='Line',
        # strategy='MultiRecord',
        instance_count=1,
        instance_type='ml.c4.xlarge',
        output_path=output_location
    )
    # Start a transform job
    transformer.transform(input_file, content_type='text/csv', split_type='Line')
    # Then wait until the transform job has completed
    transformer.wait()
    return output_location


def git_hash():
    return subprocess.check_output(["git", "describe", "--always"]).strip().decode('utf-8')

