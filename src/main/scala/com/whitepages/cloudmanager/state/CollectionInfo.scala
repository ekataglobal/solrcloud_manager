package com.whitepages.cloudmanager.state

// Try to pretend metadata about a collection is immutable cluster state too, even though it
// isn't immutable and has a per-lookup cost. (Also, sigh)
case class CollectionInfo(private val configLookup: (String) => String) {

  //TODO: Memoize
  def configName(collection: String) = configLookup(collection)
}

