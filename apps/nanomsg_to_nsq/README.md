ps: How to build Libnanomsg
1.Download nanomsg/nanomsg
2.install autoconf & libtool
3.run autogen.sh
4../configure
5.make
6.make install


to build the program depend on the nanomsg, you need to config the 
PKG_CONFIG path to the config libnanomsg.pc file

PKG_CONFIG_PATH=/usr/local/lib/pkgconfig/

If program cannot find the dlibe add the env

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/