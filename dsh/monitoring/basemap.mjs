// Keep adjacent longitudes continuous before geojson-vt clips/wraps the world tile.
export function unwrapRing(ring) {
  let previous;
  return ring.map(([longitude, latitude]) => {
    let x = longitude;
    if (previous !== undefined) {
      while (x - previous > 180) x -= 360;
      while (x - previous < -180) x += 360;
    }
    previous = x;
    return [x, latitude];
  });
}
export function unwrapWorld(collection) {
  return {
    ...collection,
    features: collection.features.map((f) => ({
      ...f,
      geometry: {
        ...f.geometry,
        coordinates:
          f.geometry.type === "Polygon"
            ? f.geometry.coordinates.map(unwrapRing)
            : f.geometry.coordinates.map((p) => p.map(unwrapRing)),
      },
    })),
  };
}
