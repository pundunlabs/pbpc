#!/bin/sh
cd include

REPOSRC=https://github.com/erdemaksu/apollo.git
if [ ! -d apollo ]
then
    git clone $REPOSRC apollo
else
    (cd apollo && git pull $REPOSRC)
fi

# Create a symbolic link so gpb plugin can access gpb.hrl
MY_LINK=gpb.hrl
if [ -L $MY_LINK ]; then
    rm $MY_LINK
fi
ln -s $REBAR_PLUGINS_DIR/gpb/include/gpb.hrl $MY_LINK
# End
