package com.whitepages.cloudmanager

class ManagerException(msg: String) extends RuntimeException(msg)
class OperationsException(msg: String) extends ManagerException(msg)
