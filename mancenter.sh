docker pull tomcat:9.0.82-jre8
docker run -d -p 8080:8080 --name Mytomcat tomcat:9.0.82-jre8
docker cp ~/Downloads/management-center-3.8.5/mancenter-3.8.5.war Mytomcat:/usr/local/tomcat/webapps/