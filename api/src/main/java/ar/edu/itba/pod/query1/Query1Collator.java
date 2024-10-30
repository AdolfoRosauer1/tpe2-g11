package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Collator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1Collator implements Collator<Map.Entry<String, Integer>, List<String>> {
    @Override
    public List<String> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        return (StreamSupport.stream(iterable.spliterator(), false)
                .sorted((e1, e2) -> {
                    int compare = e2.getValue().compareTo(e1.getValue());
                    if (compare == 0) {
                        String[] parts1 = e1.getKey().split(";");
                      String[] parts2 = e2.getKey().split(";");
                      int infrCompare = parts1[0].compareTo(parts2[0]);
                      if (infrCompare != 0) return infrCompare;
                        return parts1[1].compareTo(parts2[1]);
                    }
                    return compare;
                        }

                )
                .filter(e -> e.getValue() > 0)
                .map(e -> e.getKey() + ";" + e.getValue())
                .collect(Collectors.toList()));
    }
}

//@Override
//public List<String> collate(Iterable<Map.Entry<String, Integer>> entries) {
//    // Convertimos las entradas a una lista y las ordenamos
//    return StreamSupport.stream(entries.spliterator(), false)
//            .sorted((e1, e2) -> {
//                // Orden descendente por cantidad de multas
//                int compare = e2.getValue().compareTo(e1.getValue());
//                if (compare != 0) return compare;
//
//                // Desempate alfabético por infracción y agencia
//                String[] parts1 = e1.getKey().split(";");
//                String[] parts2 = e2.getKey().split(";");
//                int infrCompare = parts1[0].compareTo(parts2[0]);
//                if (infrCompare != 0) return infrCompare;
//
//                return parts1[1].compareTo(parts2[1]);
//            })
//            .map(e -> e.getKey() + ";" + e.getValue())  // Formato "Infracción;Agencia;Cantidad"
//            .collect(Collectors.toList());
//}
