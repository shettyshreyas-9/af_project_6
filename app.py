import os
import subprocess
import platform
import sys
import time

def run_docker_compose():

    
    try:
        # Stop and remove all running containers and networks
        subprocess.run(["docker-compose", "down", "--volumes", "--remove-orphans"], check=True)
        print("Waiting for docker-compose down to complete...")
        time.sleep(30)  # Adjust the sleep time as needed

        # # Remove all stopped containers
        # try:
        #     containers = subprocess.check_output("docker ps -aq", shell=True).decode('utf-8').strip()
        #     if containers:
        #         subprocess.run(f"docker rm -f {containers}", shell=True, check=True)
        #         print("All stopped containers removed.")
        #         time.sleep(10)
        #     else:
        #         print("No stopped containers to remove.")
        # except subprocess.CalledProcessError as e:
        #     print(f"Failed to execute docker rm command: {e}")

        # # Remove all Docker images
        # try:
        #     images = subprocess.check_output("docker images -q", shell=True).decode('utf-8').strip()
        #     if images:
        #         subprocess.run(f"docker rmi -f {images}", shell=True, check=True)
        #         print("All Docker images removed.")
        #         time.sleep(10)
        #     else:
        #         print("No Docker images to remove.")
        # except subprocess.CalledProcessError as e:
        #     print(f"Failed to execute docker rmi command: {e}")

        # Prune all unused containers, networks, images, and volumes
        try:
            # Remove all unused containers, networks, images, and volumes
            subprocess.run(["docker", "system", "prune", "-f", "--volumes"], check=True)
            print("System prune executed successfully.")
            time.sleep(10)  # Optional: wait for a bit to allow the system to settle
        except subprocess.CalledProcessError as e:
            print(f"Failed to execute system prune: {e}")


    except subprocess.CalledProcessError as e:
        print(f"Failed to execute command: {e}")


    try:
        # Remove all unused containers, networks, images, and volumes
        subprocess.run(["docker", "system", "prune", "-f", "--volumes"], check=True)
        print("System prune executed successfully.")
        time.sleep(10)  # Optional: wait for a bit to allow the system to settle
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute system prune: {e}")


    # time.sleep(10)
    # subprocess.run(["docker-compose", "build"], check=True)
    # time.sleep(10)
    # print('docker-compose build executed successfully')

    # try:
    #     # Execute the docker-compose up --build command
    #     subprocess.run(["docker-compose", "up","airflow-init"], check=True)

    #     print("Waiting for initialization to complete...")
    #     time.sleep(30)  # Adjust the sleep time as needed

    # except subprocess.CalledProcessError as e:
    #     print(f"Failed to execute docker-compose airflow init: {e}")

    try:
        # Execute the docker-compose up --build command
        subprocess.run(["docker-compose", "up", "--build"], check=True)

        # Wait for services to be ready
        print("Waiting for services to be ready...")
        time.sleep(30)  # Adjust the sleep time as needed

    except subprocess.CalledProcessError as e:
        print(f"Failed to execute docker-compose: {e}")

    # Copy files to Airflow containers
    copy_files_to_container("app-airflow-webserver-1")
    copy_files_to_container("app-airflow-scheduler-1")
    copy_files_to_container("app-airflow-worker-1")
    copy_files_to_container("app-airflow-triggerer-1")
 


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

    while True:
        pass