#!/bin/sh
cd include
git clone https://github.com/erdemaksu/apollo.git

# Create a symbolic link so gpb plugin can access gpb.hrl
MY_LINK=gpb
if [ -L $MY_LINK ]; then
    rm $MY_LINK
fi
ln -s $REBAR_PLUGINS_DIR/gpb/include $MY_LINK
# End
