FROM gsv:0.7.0-ubuntu

ADD ./ /app/one_pass/src
RUN cd /app/one_pass/src && python3 setup.py bdist_wheel && pip3 wheel . && pip3 install one_pass*.whl 
ENTRYPOINT ["python3"]
