/* ---------------------------------------------------------------
Práctica 1.
Código fuente: ShuffleTuple.java
Grau Informàtica
NIF [pendiente] y nombre completo autor1: Augustin-Dragos Belu.
NIF [pendiente] y nombre completo autor2: Abraham Ruíz Monsteirin.
--------------------------------------------------------------- */

// En esta práctica los valores emitidos por Map son conteos positivos.
// Se reserva un valor negativo para indicar el fin de flujo de Shuffle.
record ShuffleTuple(String key, int value) {
    boolean end() {
        return value < 0;
    }

    static ShuffleTuple endOfStream() {
        return new ShuffleTuple("", -1);
    }
}
