#include "StdAfx.h"

#import "mscorlib.tlb"
#import "C:\OASIS\libs\TimeSeriesLibrary.tlb"

using namespace TimeSeriesLibrary;

void main()
{
    //CoInitialize(NULL);
    //TimeSeriesLibrary::ComTSLibrary __uuidof(TimeSeriesLibrary::ComTSLibrary);

    TimeSeriesLibrary::_ComTSLibrary *TSLib;
    CoInitialize(NULL);
    TimeSeriesLibrary::_ComTSLibraryPtr p(__uuidof(TimeSeriesLibrary::ComTSLibrary));
    TSLib = p;



}