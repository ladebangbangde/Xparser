package com.ldbbd.xparser.interfaces;

import com.ldbbd.xparser.operations.Operation;

import java.util.Optional;

public interface Converter {
    public Optional<Operation>convert();
}
