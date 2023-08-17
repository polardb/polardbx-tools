FROM repository.dayu.work/frodoplus/java-python:v1

# 大禹工作流会自动根据pom采用mvn clean install -DskipTests 打出jar包
ADD ./target/frodo-1.1.17.jar /app/frodo.jar
ADD ./*.py /app/
ADD ./*.sh /app/
ADD ./license.key /app/
ADD ./index /app/index/

CMD ["python", "/app/index/app.py"]