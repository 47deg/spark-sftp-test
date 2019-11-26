package org.training.trainingbot
package config

import cats.effect.Sync
import cats.syntax.either._
import pureconfig.{ConfigReader, ConfigSource, Derivation}

trait ConfigLoader[F[_]] {

  def loadConfig[Config](implicit reader: Derivation[ConfigReader[Config]]): F[Config]

}

object ConfigLoader {
  def apply[F[_]: Sync]: ConfigLoader[F] = new ConfigLoader[F] {

    override def loadConfig[Config](implicit reader: Derivation[ConfigReader[Config]]): F[Config] =
      Sync[F].fromEither(
        ConfigSource.default
          .load[Config]
          .leftMap(e => new IllegalStateException(s"Error loading configuration: $e"))
      )

  }
}
