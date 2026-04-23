FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./extractor /code/app

# OpenShift arbitrary-UID compatibility: every runtime UID is non-root
# and in GID 0. Make /code group-owned by 0 and group-writable so the
# runtime UID (whatever it is) can read/write app files.
RUN chgrp -R 0 /code && chmod -R g=u /code

# Numeric USER so OpenShift's arbitrary-UID policy honours it. OKD
# will override the specific UID at runtime; the chgrp/chmod above
# ensures whichever UID lands in this container (always in GID 0) can
# read and write /code.
USER 1001

# Port 8080 instead of 80 — arbitrary non-root UIDs can't bind < 1024
# on OpenShift without extra SCCs.
EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

