import React, { useEffect, useRef } from "react";
import mapboxgl from "mapbox-gl";
import "./Map.css";

mapboxgl.accessToken = "<YOUR_TOKEN_HERE>";

const Map = () => {
  const mapContainerRef = useRef(null);

  // Initialize map when component mounts
  useEffect(() => {
    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: "/style.json",
      center: [-94, 38],
      minZoom: 2,
      zoom: 4
    });

    // Add zoom and rotation controls to the map.
    map.addControl(new mapboxgl.NavigationControl());

    map.on("load", function() {
      // optional 3d terrain, you will need hillshading as well
      // map.addSource("mapbox-dem", {
      //   "type": "raster-dem",
      //   "url": "mapbox://mapbox.mapbox-terrain-dem-v1",
      //   "tileSize": 512,
      //   "maxzoom": 14
      // });

      // // add the DEM source as a terrain layer with exaggerated height
      // map.setTerrain({ "source": "mapbox-dem", "exaggeration": 1.5 });
    });

    // Clean up on unmount
    return () => map.remove();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div>
      <div className="map-container" ref={mapContainerRef} />
    </div>
  );
};

export default Map;