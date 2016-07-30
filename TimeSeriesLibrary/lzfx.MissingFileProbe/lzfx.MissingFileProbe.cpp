
// The purpose of this application is to be a COM-based application that calls the lzfx.dll
// file.  It is only intended to be used when lzfx has a missing dependency.  If there are
// missing dependencies, then Windows will tell the user what those missing dependencies are (e.g. 
// "the file AXA32.DLL is missing").  Therefore, we created this application because Windows only reports
// this useful information when loading a COM-based application.  When lzfx is loaded by a .Net-based
// application, it reports that there were missing dependencies, but it does not identify them.

#include "stdafx.h"
#include "..\lzfx\lzfx.h"

/// This is the entry-point function for the executable
int APIENTRY _tWinMain(HINSTANCE hInstance,
                     HINSTANCE hPrevInstance,
                     LPTSTR    lpCmdLine,
                     int       nCmdShow)
{
    char ibuf[16], obuf[16];
    // Call lzfx function, which does nothing useful other than to
    // prove whether any function can be called.
    int n = lzfx_compress((void*)(*ibuf), 0, (void*)(*obuf), 0);
    // If we successfully return from the lzfx function, then exit 
    // with a distinctive code that the caller can recognize.  If the caller sees
    // this code, then it indicates that there was no missing dependency error.
    ExitProcess(23445);
}



