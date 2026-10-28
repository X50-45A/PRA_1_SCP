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
