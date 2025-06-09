# tpch-query5_submission

## Introduction
This is the project submission, made by doing git clone of **https://github.com/zettagrv/tpch-query5**

## File Structure
```
.
├── build
│   ├── CMakeFiles
│   ├── cmake_install.cmake
│   ├── Makefile
│   ├── old
│   └── tpch_query5
├── CMakeLists.txt
├── include
│   └── query5.hpp
├── README.md
├── result
│   ├── four-thread-output.txt
│   └── single-thread-output.txt
├── runtimes-singlle_and_four_threads.txt
├── screenshots
│   ├── four-thread-processing-screenshot.png
│   └── single-thread-processing-screenshot.png
└── src
    ├── backup
    ├── main.cpp
    └── query5.cpp
```
## Important Node
If we see the runtimes of 4 thread and 1 thread operations, we find that the 1-threaded one is faster than 4-threaded one. This seemed unusual to me in the beginning. Therefore, I decided to do some experimentations and come up with the reason for it.
The entire reasoning is in the pdf ```issue_report.pdf```.
