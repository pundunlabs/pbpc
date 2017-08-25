#!/bin/sh
cd include
APOLLO_TAG="v1.0.4"
REPOSRC=https://github.com/erdemaksu/apollo.git
LOCALREPO=apollo
LOCALREPO_VC_DIR=$LOCALREPO/.git

if [ ! -d $LOCALREPO_VC_DIR ]
then
    git clone $REPOSRC $LOCALREPO
else
    (cd $LOCALREPO && git pull $REPOSRC --tags)
fi

(cd $LOCALREPO && git checkout $APOLLO_TAG 2>/dev/null || git checkout -b $APOLLO_TAG $APOLLO_TAG)

# Create a symbolic link so gpb plugin can access gpb.hrl
MY_LINK=gpb.hrl
if [ -L $MY_LINK ]; then
    rm $MY_LINK
fi
ln -s $REBAR_PLUGINS_DIR/gpb/include/gpb.hrl $MY_LINK
# End
