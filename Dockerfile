FROM fishtownanalytics/dbt:1.0.0
WORKDIR /support
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/near
WORKDIR /near
COPY . .
EXPOSE 8080
ENTRYPOINT [ "bash"]