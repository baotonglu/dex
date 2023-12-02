#include "example.h"
#include <cstdio>

template<class T, class P>
Example<T, P>::Example(T a){
    a_ = a;
}

template<class T, class P>
void Example<T, P>::helloword(){
    printf("Hello! %d\n", a_);
}