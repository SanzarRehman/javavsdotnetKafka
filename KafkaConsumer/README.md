Docker Command Lines:

Build:
----------------------
docker build -t spring .


Run:
--------------------------------
[With memory capped]
docker run --rm --network host --memory 0000m spring

[With cpu capped]
docker run --rm --network host --cpus=0.0 spring

[No Cap]
docker run --rm --network host spring