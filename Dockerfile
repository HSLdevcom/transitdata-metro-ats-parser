FROM hsldevcom/infodevops-docker-base-images:1.0.0-java-25

ADD target/transitdata-metro-ats-parser-jar-with-dependencies.jar /usr/app/transitdata-metro-ats-parser.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]
