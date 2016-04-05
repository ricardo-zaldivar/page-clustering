package edu.usc.irds.autoext.spark

import org.apache.hadoop.conf.Configuration
import org.apache.nutch.protocol.Content

/**
  * Created by tg on 4/5/16.
  */
object Utils {

  def cloneContent(in:Content) : Content = {
    new Content(in.getUrl, in.getBaseUrl, in.getContent,
      in.getContentType, in.getMetadata, new Configuration())
  }

}
