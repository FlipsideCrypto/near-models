FROM fishtownanalytics/dbt:0.21.1
WORKDIR /support
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/near
WORKDIR /near
COPY . .
EXPOSE 8080
ENTRYPOINT [ "bash"]