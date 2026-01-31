#version 300 es
precision highp float;

// Blur type constants
const int BLUR_GAUSSIAN = 0;
const int BLUR_BOX = 1;
const int BLUR_RADIAL = 2;

// Radial blur config
const int RADIAL_SAMPLES = 12;
const float RADIAL_STRENGTH = 0.0003;

uniform sampler2D u_image0;
uniform vec2 u_resolution;
uniform int u_int0;      // Blur type (BLUR_GAUSSIAN, BLUR_BOX, BLUR_RADIAL)
uniform float u_float0;  // Blur radius/amount

in vec2 v_texCoord;
layout(location = 0) out vec4 fragColor0;

float gaussian(float x, float sigma) {
    return exp(-(x * x) / (2.0 * sigma * sigma));
}

void main() {
    vec2 texelSize = 1.0 / u_resolution;
    float radius = max(u_float0, 0.0);
    
    // Radial (angular) blur
    if (u_int0 == BLUR_RADIAL) {
        vec2 center = vec2(0.5);
        vec2 dir = v_texCoord - center;
        float dist = length(dir);
        
        // Avoid division by zero
        if (dist < 1e-4) {
            fragColor0 = texture(u_image0, v_texCoord);
            return;
        }
        
        vec4 sum = vec4(0.0);
        float totalWeight = 0.0;
        float angleStep = radius * RADIAL_STRENGTH;
        
        dir /= dist;
        
        for (int i = -RADIAL_SAMPLES; i <= RADIAL_SAMPLES; i++) {
            float a = float(i) * angleStep;
            float s = sin(a);
            float c = cos(a);
            vec2 rotatedDir = vec2(
                dir.x * c - dir.y * s,
                dir.x * s + dir.y * c
            );
            vec2 uv = center + rotatedDir * dist;
            float w = 1.0 - abs(float(i)) / float(RADIAL_SAMPLES);
            sum += texture(u_image0, uv) * w;
            totalWeight += w;
        }
        
        fragColor0 = sum / totalWeight;
        return;
    }
    
    // Gaussian / Box blur
    int samples = int(ceil(radius));
    
    if (samples == 0) {
        fragColor0 = texture(u_image0, v_texCoord);
        return;
    }
    
    vec4 color = vec4(0.0);
    float totalWeight = 0.0;
    float sigma = radius / 2.0;
    
    for (int x = -samples; x <= samples; x++) {
        for (int y = -samples; y <= samples; y++) {
            vec2 offset = vec2(float(x), float(y)) * texelSize;
            vec4 sample_color = texture(u_image0, v_texCoord + offset);
            
            float weight;
            if (u_int0 == BLUR_GAUSSIAN) {
                float dist = length(vec2(float(x), float(y)));
                weight = gaussian(dist, sigma);
            } else {
                // BLUR_BOX
                weight = 1.0;
            }
            
            color += sample_color * weight;
            totalWeight += weight;
        }
    }
    
    fragColor0 = color / totalWeight;
}
