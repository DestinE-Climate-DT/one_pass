FROM mambaorg/micromamba:1.4.7

WORKDIR /app/

COPY --chown=$MAMBA_USER:$MAMBA_USER . one_pass

ENV ENV_NAME=env_opa

# gcc because pytdigest is not picking up the wheel
# git for deps installed with git...
    
RUN micromamba env create -n $ENV_NAME -c conda-forge gcc vim git &&\
    micromamba install -y -n $ENV_NAME -f one_pass/environment.yml &&\
    micromamba clean --all --yes

WORKDIR /app/one_pass/

CMD ["python", "-c", "'import one_pass'"]

