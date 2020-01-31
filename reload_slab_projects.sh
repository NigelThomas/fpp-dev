!#/bin/bash
#
echo Reload slab files into development environment
# assumes we are in the correct directory /home/sqlstream/<project>

for slab in *.slab
do
    if [ -e $slab ]
    then
        echo ... ... importing $slab
        echo 
        curl -H 'Content-Type: application/json' -d @$slab http://localhost:5585/_project/user/${slab_project}
    else
        echo no file $slab
        break
    fi
done
