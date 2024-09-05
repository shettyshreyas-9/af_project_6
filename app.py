import os
import subprocess
import platform
import sys
import time

def run_docker_compose():
    try:
        # Execute the docker-compose up --build command
        subprocess.run(["docker-compose", "up","airflow-init"], check=True)

        print("Waiting for initialization to complete...")
        time.sleep(30)  # Adjust the sleep time as needed

    except subprocess.CalledProcessError as e:
        print(f"Failed to execute docker-compose airflow init: {e}")

    try:
        # Execute the docker-compose up --build command
        subprocess.run(["docker-compose", "up","-d"], check=True)

        # Wait for services to be ready
        print("Waiting for services to be ready...")
        time.sleep(30)  # Adjust the sleep time as needed

    except subprocess.CalledProcessError as e:
        print(f"Failed to execute docker-compose: {e}")

    # Copy files to Airflow containers
    copy_files_to_container("app-airflow-webserver-1")
    copy_files_to_container("app-airflow-scheduler-1")
    copy_files_to_container("app-airflow-worker-1")


def copy_files_to_container(container_name):
    try:
        # Replace the following with the path to the files you want to copy
        source_path = "/app/dags"
        
        # Copy files to the specified container
        subprocess.run(["docker", "cp", source_path, f"{container_name}:/opt/airflow/"], check=True)
        print(f"Files copied to {container_name}")
    
    except subprocess.CalledProcessError as e:
        print(f"Failed to copy files to {container_name}: {e}")


if __name__ == "__main__":
    run_docker_compose()