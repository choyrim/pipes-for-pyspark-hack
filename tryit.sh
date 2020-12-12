#! bash

cat <<__END__
# to try it out
cd work
pip install -r requirements.txt
pytest tests
__END__
docker run -it -v "$PWD:/home/jovyan/work" jupyter/pyspark-notebook:42f4c82a07ff /bin/bash

