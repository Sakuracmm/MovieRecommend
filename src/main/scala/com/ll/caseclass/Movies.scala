package com.ll.caseclass

case class Movies(movieId: Int, title: String, genres: String) {
  def getMovieId: Int = {
    this.movieId
  }
  def getTitle: String = {
    this.title
  }
  def getGenres: String = {
    this.genres
  }
}
