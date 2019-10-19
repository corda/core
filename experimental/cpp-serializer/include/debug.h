#pragma once

/******************************************************************************/

#define AMQP_DEBUG 1

/******************************************************************************/

#if defined AMQP_DEBUG && AMQP_DEBUG >= 1
    #define DBG(X) std::cout << X
#else
    #define DBG(X)
#endif

/******************************************************************************/
