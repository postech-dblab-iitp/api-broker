/*
 * Copyright 2024 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

#include <windows.h>

#define VER_FILEVERSION @API_BROKER_MAJOR_VERSION@,@API_BROKER_MINOR_VERSION@,@API_BROKER_PATCH_VERSION@,@API_BROKER_EXTRA_VERSION@
#define VER_FILEVERSION_STR "@API_BROKER_MAJOR_VERSION@.@API_BROKER_MINOR_VERSION@.@API_BROKER_PATCH_VERSION@.@API_BROKER_EXTRA_VERSION@\0" 
#define VER_PRODUCTVERSION @API_BROKER_MAJOR_VERSION@,@API_BROKER_MINOR_VERSION@,@API_BROKER_PATCH_VERSION@,0
#define VER_PRODUCTVERSION_STR "@API_BROKER_MAJOR_VERSION@.@API_BROKER_MINOR_VERSION@.@API_BROKER_PATCH_VERSION@\0" 

VS_VERSION_INFO VERSIONINFO
FILEVERSION     VER_FILEVERSION
PRODUCTVERSION  VER_PRODUCTVERSION
FILEFLAGSMASK   VS_FFI_FILEFLAGSMASK
#ifdef _DEBUG
  FILEFLAGS VS_FF_DEBUG
#else
  FILEFLAGS 0x0L
#endif
FILEOS          VOS__WINDOWS32
FILETYPE        VFT_APP
FILESUBTYPE     VFT2_UNKNOWN
BEGIN
  BLOCK "StringFileInfo"
  BEGIN
    BLOCK "040904B0"
    BEGIN
      VALUE "CompanyName", "CUBRID"
      VALUE "FileVersion", VER_FILEVERSION_STR
      VALUE "LegalCopyright", "Copyright (C) 2024 CUBRID. All rights reserved."
      VALUE "ProductName", "API_BROKER"
      VALUE "ProductVersion", VER_PRODUCTVERSION_STR
    END
  END
  BLOCK "VarFileInfo"
  BEGIN
    VALUE "Translation", 0x409, 1252
  END
END
