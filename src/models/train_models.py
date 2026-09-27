"""
train_models.py — Entrenamiento y Evaluación de Modelos de Machine Learning

Este módulo implementa:
1. Carga de dataset_ml_features.csv con validación de integridad.
2. Split temporal out-of-time: Train (2023-2024, 220 filas) y Test (2025, 120 filas).
3. Preprocesamiento con ColumnTransformer:
   - StandardScaler para variables numéricas (rezagos t-1 y t-2).
   - OneHotEncoder(drop='first', sparse_output=False) para variables categóricas (banco_id, tipo_credito_id).
4. Comparación obligatoria de clasificación sobre 'riesgo_alto':
   - Regresión Logística (Baseline).
   - KNN Classifier (Avanzado).
   - Métricas: Accuracy, Precision, Recall, F1-Score, ROC-AUC, Matriz de Confusión.
5. Análisis complementario de regresión sobre 'morosidad_t' continua:
   - Regresión Lineal (análisis de factores de riesgo y persistencia temporal).
   - Métricas: R², MAE, RMSE y coeficientes explicativos.
6. Generación de artefactos visuales comparativos en la carpeta 'Graficas_Comparativas_Modelos/'.
"""

import os
import pandas as pd
import numpy as np

# Configurar backend headless para matplotlib antes de importar pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report,
    r2_score,
    mean_absolute_error,
    root_mean_squared_error
)

import sys
from pathlib import Path

# Permitir ejecuciones directas tanto como script (python src/models/train_models.py) como modulo (-m)
directorio_raiz = Path(__file__).resolve().parent.parent.parent
if str(directorio_raiz) not in sys.path:
    sys.path.insert(0, str(directorio_raiz))

try:
    from src.models.decision_rules import aplicar_capa_decision, generar_resumen_decisiones
except ImportError:
    from decision_rules import aplicar_capa_decision, generar_resumen_decisiones


def generar_graficas_comparativas(
    df_res_clf,
    cms_clf,
    y_test_clf,
    preds_clf,
    probs_clf,
    y_test_reg,
    pred_reg,
    metricas_reg,
    coeficientes_reg,
    output_dir="Graficas_Comparativas_Modelos"
):
    """
    Genera y guarda en disco las figuras de diagnóstico y comparación de modelos.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # -------------------------------------------------------------
    # 1. Gráfico de barras comparativo de métricas de clasificación
    # -------------------------------------------------------------
    metricas_cols = ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC']
    m_lr = df_res_clf.loc[df_res_clf['Modelo'].str.contains('Logística'), metricas_cols].values[0]
    m_knn = df_res_clf.loc[df_res_clf['Modelo'].str.contains('KNN'), metricas_cols].values[0]
    
    x = np.arange(len(metricas_cols))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.bar(x - width/2, m_lr, width, label='Regresión Logística (Baseline)', color='#3B82F6', edgecolor='#1D4ED8')
    bars2 = ax.bar(x + width/2, m_knn, width, label='KNN Classifier (Avanzado, k=5)', color='#10B981', edgecolor='#047857')
    
    ax.set_title('Comparativa de Rendimiento: Clasificación de Riesgo Alto\n(Test Out-of-Time: 2025)', fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel('Puntaje (Score)', fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(metricas_cols, fontsize=11, fontweight='bold')
    ax.legend(fontsize=11, loc='upper right')
    ax.set_ylim(0, 1.05)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    
    for bar in bars1:
        yval = bar.get_height()
        ax.annotate(f'{yval:.3f}', xy=(bar.get_x() + bar.get_width()/2, yval), xytext=(0, 4), textcoords='offset points', ha='center', va='bottom', fontsize=9, fontweight='bold')
    for bar in bars2:
        yval = bar.get_height()
        ax.annotate(f'{yval:.3f}', xy=(bar.get_x() + bar.get_width()/2, yval), xytext=(0, 4), textcoords='offset points', ha='center', va='bottom', fontsize=9, fontweight='bold')
        
    plt.tight_layout()
    ruta_g1 = os.path.join(output_dir, '01_comparativa_metricas_clasificacion.png')
    plt.savefig(ruta_g1, dpi=300)
    plt.close()
    
    # -------------------------------------------------------------
    # 2. Matrices de confusión lado a lado
    # -------------------------------------------------------------
    cm_lr = cms_clf['Regresión Logística (Baseline)']
    cm_knn = cms_clf['KNN Classifier (Avanzado, k=5)']
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    labels = ['Normal (0)', 'Riesgo Alto (1)']
    
    for ax, cm, title in zip(axes, [cm_lr, cm_knn], ['Regresión Logística (Baseline)', 'KNN Classifier (Avanzado, k=5)']):
        im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
        ax.set_title(title, fontsize=13, fontweight='bold', pad=10)
        tick_marks = np.arange(len(labels))
        ax.set_xticks(tick_marks)
        ax.set_xticklabels(labels, fontsize=10)
        ax.set_yticks(tick_marks)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel('Predicción del Modelo', fontsize=11, fontweight='bold')
        ax.set_ylabel('Clase Real (SBS)', fontsize=11, fontweight='bold')
        
        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                val = cm[i, j]
                pct = val / cm.sum() * 100
                ax.text(j, i, f'{val}\n({pct:.1f}%)', ha='center', va='center', color='white' if val > thresh else 'black', fontsize=11, fontweight='bold')
                
    fig.suptitle('Matrices de Confusión — Test Out-of-Time 2025 (N=120)', fontsize=15, fontweight='bold', y=1.02)
    plt.tight_layout()
    ruta_g2 = os.path.join(output_dir, '02_matrices_confusion.png')
    plt.savefig(ruta_g2, dpi=300, bbox_inches='tight')
    plt.close()
    
    # -------------------------------------------------------------
    # 3. Cuadro comparativo en tabla gráfica
    # -------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(11, 3.5))
    ax.axis('tight')
    ax.axis('off')
    
    tabla_datos = [
        ['Regresión Logística', 'Baseline', 'Test 2025', f"{m_lr[0]:.2%}", f"{m_lr[1]:.2%}", f"{m_lr[2]:.2%}", f"{m_lr[3]:.4f}", f"{m_lr[4]:.4f}"],
        ['KNN Classifier (k=5)', 'Avanzado', 'Test 2025', f"{m_knn[0]:.2%}", f"{m_knn[1]:.2%}", f"{m_knn[2]:.2%}", f"{m_knn[3]:.4f}", f"{m_knn[4]:.4f}"],
        ['Regresión Logística', 'Baseline', 'Train 23-24', f"{df_res_clf.loc[0, 'Train Accuracy']:.2%}", "—", "—", "—", f"{df_res_clf.loc[0, 'Train ROC-AUC']:.4f}"],
        ['KNN Classifier (k=5)', 'Avanzado', 'Train 23-24', f"{df_res_clf.loc[1, 'Train Accuracy']:.2%}", "—", "—", "—", f"{df_res_clf.loc[1, 'Train ROC-AUC']:.4f}"]
    ]
    cols = ['Modelo', 'Enfoque', 'Partición', 'Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC']
    
    tabla = ax.table(cellText=tabla_datos, colLabels=cols, cellLoc='center', loc='center')
    tabla.auto_set_font_size(False)
    tabla.set_fontsize(10)
    tabla.scale(1.1, 1.8)
    
    for (row, col), cell in tabla.get_celld().items():
        if row == 0:
            cell.set_facecolor('#1E3A8A')
            cell.set_text_props(color='white', fontweight='bold')
        elif row in [1, 2]:
            cell.set_facecolor('#F0FDF4' if row == 2 else '#EFF6FF')
        else:
            cell.set_facecolor('#F9FAFB')
        cell.set_edgecolor('#D1D5DB')
        
    plt.title('Cuadro Comparativo de Rendimiento: Modelos de Clasificación\n(Métricas Out-of-Time 2025 vs. In-Sample Train)', fontsize=13, fontweight='bold', pad=20)
    ruta_g3 = os.path.join(output_dir, '03_cuadro_comparativo_resumen.png')
    plt.savefig(ruta_g3, dpi=300, bbox_inches='tight')
    plt.close()
    
    # -------------------------------------------------------------
    # 4. Regresión lineal: morosidad real vs predicha
    # -------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(y_test_reg, pred_reg, color='#2563EB', alpha=0.7, edgecolors='k', s=60, label='Observaciones Test 2025 (N=120)')
    
    lims = [min(y_test_reg.min(), pred_reg.min()) - 0.2, max(y_test_reg.max(), pred_reg.max()) + 0.2]
    ax.plot(lims, lims, color='#DC2626', linestyle='--', linewidth=2, label='Predicción Perfecta (y = x)')
    
    ax.set_xlim(lims)
    ax.set_ylim(lims)
    ax.set_xlabel('Morosidad Real SBS (%)', fontsize=11, fontweight='bold')
    ax.set_ylabel('Morosidad Predicha por Modelo (%)', fontsize=11, fontweight='bold')
    ax.set_title('Regresión Lineal: Morosidad Real vs. Predicha\n(Test Out-of-Time: 2025)', fontsize=13, fontweight='bold', pad=15)
    ax.grid(True, linestyle=':', alpha=0.6)
    
    info_txt = f"R² Score: {metricas_reg['R2']:.4f}\nMAE: {metricas_reg['MAE']:.4f} pp\nRMSE: {metricas_reg['RMSE']:.4f} pp"
    ax.text(0.05, 0.92, info_txt, transform=ax.transAxes, fontsize=11, verticalalignment='top', bbox=dict(boxstyle='round,pad=0.5', facecolor='#FEF3C7', edgecolor='#F59E0B', alpha=0.9))
    ax.legend(loc='lower right', fontsize=10)
    
    plt.tight_layout()
    ruta_g4 = os.path.join(output_dir, '04_regresion_lineal_pred_vs_real.png')
    plt.savefig(ruta_g4, dpi=300)
    plt.close()
    
    # -------------------------------------------------------------
    # 5. Coeficientes de regresión lineal
    # -------------------------------------------------------------
    df_coef_sorted = coeficientes_reg.sort_values('Coeficiente')
    fig, ax = plt.subplots(figsize=(10, 6))
    colores = ['#DC2626' if c < 0 else '#16A34A' for c in df_coef_sorted['Coeficiente']]
    bars = ax.barh(df_coef_sorted['Feature'], df_coef_sorted['Coeficiente'], color=colores, edgecolor='black', alpha=0.85)
    
    ax.set_title('Factores Determinantes de la Morosidad (Coeficientes de Regresión Lineal)', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Magnitud del Coeficiente (Efecto en morosidad_t)', fontsize=11, fontweight='bold')
    ax.axvline(0, color='black', linewidth=1, linestyle='--')
    ax.grid(axis='x', linestyle=':', alpha=0.6)
    
    for bar in bars:
        w = bar.get_width()
        offset = 0.02 if w >= 0 else -0.02
        ha = 'left' if w >= 0 else 'right'
        ax.annotate(f'{w:.4f}', xy=(w, bar.get_y() + bar.get_height()/2), xytext=(offset*100, 0), textcoords='offset points', ha=ha, va='center', fontsize=9, fontweight='bold')
        
    plt.tight_layout()
    ruta_g5 = os.path.join(output_dir, '05_coeficientes_regresion_lineal.png')
    plt.savefig(ruta_g5, dpi=300)
    plt.close()
    
    print(f"\n🎨 Gráficas comparativas generadas exitosamente en '{output_dir}/':")
    print(f"   1. {ruta_g1}")
    print(f"   2. {ruta_g2}")
    print(f"   3. {ruta_g3}")
    print(f"   4. {ruta_g4}")
    print(f"   5. {ruta_g5}")


def entrenar_y_evaluar(ruta_data="data/processed/dataset_ml_features.csv", guardar_graficas=True, output_graficas="Graficas_Comparativas_Modelos"):
    """
    Entrena y evalúa los modelos de clasificación y regresión del proyecto.
    
    Retorna:
    --------
    dict con las métricas y modelos entrenados.
    """
    if not os.path.exists(ruta_data):
        raise FileNotFoundError(f"No se encontró el dataset en {ruta_data}. Ejecute build_features.py primero.")
        
    df = pd.read_csv(ruta_data)
    
    # Compuerta de Calidad: exclusión estricta de agregados del sistema
    assert 'Total Banca Múltiple' not in df['banco_id'].values, (
        "Error crítico: 'Total Banca Múltiple' no debe formar parte del entrenamiento."
    )
    
    # 1. Definición de variables según Sección 3 de AGENTS.md
    features_num = [
        'morosidad_lag1', 'morosidad_lag2',
        'tasa_activa_lag1', 'tipo_cambio_lag1',
        'tasa_referencia_lag1', 'inflacion_lag1'
    ]
    features_cat = ['banco_id', 'tipo_credito_id']
    
    target_clf = 'riesgo_alto'
    target_reg = 'morosidad_t' if 'morosidad_t' in df.columns else 'morosidad'
    
    # 2. Split temporal estricto (no aleatorio)
    # Train: 2023-03 a 2024-12 (22 meses × 10 series = 220 observaciones)
    # Test: 2025-01 a 2025-12 (12 meses × 10 series = 120 observaciones)
    mask_train = df['fecha_id'] <= '2024-12'
    mask_test = df['fecha_id'] >= '2025-01'
    
    X_train = df.loc[mask_train, features_num + features_cat]
    X_test = df.loc[mask_test, features_num + features_cat]
    
    y_train_clf = df.loc[mask_train, target_clf]
    y_test_clf = df.loc[mask_test, target_clf]
    
    y_train_reg = df.loc[mask_train, target_reg]
    y_test_reg = df.loc[mask_test, target_reg]
    
    print("=" * 75)
    print("PARTICIÓN TEMPORAL DEL DATASET (PANEL BANCARIO)")
    print("=" * 75)
    print(f"Train (2023-03 a 2024-12): {len(X_train)} filas ({len(X_train)/len(df):.1%})")
    print(f"Test  (2025-01 a 2025-12): {len(X_test)} filas ({len(X_test)/len(df):.1%})")
    print(f"Distribución target riesgo_alto en Train: {dict(y_train_clf.value_counts())}")
    print(f"Distribución target riesgo_alto en Test:  {dict(y_test_clf.value_counts())}")
    
    # 3. Pipeline de preprocesamiento (ColumnTransformer)
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), features_num),
            ('cat', OneHotEncoder(drop='first', sparse_output=False), features_cat)
        ]
    )
    
    # 4. TAREA PRINCIPAL: Comparación de Clasificación (riesgo_alto)
    # Baseline: Regresión Logística vs Avanzado: KNN Classifier
    clasificadores = {
        'Regresión Logística (Baseline)': LogisticRegression(random_state=42, max_iter=1000),
        'KNN Classifier (Avanzado, k=5)': KNeighborsClassifier(n_neighbors=5)
    }
    
    resultados_clf = []
    modelos_entrenados = {}
    preds_clf = {}
    probs_clf = {}
    cms_clf = {}
    
    print("\n" + "=" * 75)
    print("COMPARACIÓN OBLIGATORIA: REGRESIÓN LOGÍSTICA vs. KNN CLASSIFIER")
    print("Target: riesgo_alto | Split: Out-of-Time (Test 2025)")
    print("=" * 75)
    
    for nombre, clf in clasificadores.items():
        pipe = Pipeline([('prep', preprocessor), ('model', clf)])
        pipe.fit(X_train, y_train_clf)
        modelos_entrenados[nombre] = pipe
        
        # Predicciones en Test
        y_pred = pipe.predict(X_test)
        y_prob = pipe.predict_proba(X_test)[:, 1]
        
        preds_clf[nombre] = y_pred
        probs_clf[nombre] = y_prob
        
        # Métricas Test
        acc = accuracy_score(y_test_clf, y_pred)
        prec = precision_score(y_test_clf, y_pred, zero_division=0)
        rec = recall_score(y_test_clf, y_pred, zero_division=0)
        f1 = f1_score(y_test_clf, y_pred, zero_division=0)
        auc = roc_auc_score(y_test_clf, y_prob)
        cm = confusion_matrix(y_test_clf, y_pred)
        cms_clf[nombre] = cm
        
        # Métricas Train (para control de generalización)
        y_pred_tr = pipe.predict(X_train)
        y_prob_tr = pipe.predict_proba(X_train)[:, 1]
        acc_tr = accuracy_score(y_train_clf, y_pred_tr)
        auc_tr = roc_auc_score(y_train_clf, y_prob_tr)
        
        resultados_clf.append({
            'Modelo': nombre,
            'Accuracy': acc,
            'Precision': prec,
            'Recall': rec,
            'F1-Score': f1,
            'ROC-AUC': auc,
            'Train Accuracy': acc_tr,
            'Train ROC-AUC': auc_tr
        })
        
        print(f"\n>>> {nombre} <<<")
        print(f"Accuracy:  {acc:.4f}  (Train: {acc_tr:.4f})")
        print(f"Precision: {prec:.4f}")
        print(f"Recall:    {rec:.4f}")
        print(f"F1-Score:  {f1:.4f}")
        print(f"ROC-AUC:   {auc:.4f}  (Train: {auc_tr:.4f})")
        print(f"Matriz de Confusión:\n{cm}")
        print("\nReporte de Clasificación:")
        print(classification_report(y_test_clf, y_pred, zero_division=0))
        
    df_res_clf = pd.DataFrame(resultados_clf)
    print("\n--- RESUMEN COMPARATIVO DE CLASIFICACIÓN (TEST 2025) ---")
    print(df_res_clf[['Modelo', 'Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC']].to_string(index=False))
    
    # 5. TAREA COMPLEMENTARIA: Regresión Lineal sobre morosidad_t
    print("\n" + "=" * 75)
    print("ANÁLISIS COMPLEMENTARIO: REGRESIÓN LINEAL SOBRE morosidad_t CONTINUA")
    print("Target continuo: morosidad_t | Split: Out-of-Time (Test 2025)")
    print("=" * 75)
    
    pipe_lin = Pipeline([('prep', preprocessor), ('reg', LinearRegression())])
    pipe_lin.fit(X_train, y_train_reg)
    modelos_entrenados['Regresión Lineal'] = pipe_lin
    
    y_pred_reg = pipe_lin.predict(X_test)
    y_pred_reg_tr = pipe_lin.predict(X_train)
    
    r2_test = r2_score(y_test_reg, y_pred_reg)
    mae_test = mean_absolute_error(y_test_reg, y_pred_reg)
    rmse_test = root_mean_squared_error(y_test_reg, y_pred_reg)
    
    r2_train = r2_score(y_train_reg, y_pred_reg_tr)
    mae_train = mean_absolute_error(y_train_reg, y_pred_reg_tr)
    rmse_train = root_mean_squared_error(y_train_reg, y_pred_reg_tr)
    
    print(f"R² Score (Test):  {r2_test:.4f}  (Train: {r2_train:.4f})")
    print(f"MAE (Test):       {mae_test:.4f}  (Train: {mae_train:.4f})")
    print(f"RMSE (Test):      {rmse_test:.4f}  (Train: {rmse_train:.4f})")
    
    # Extracción de nombres de features tras ColumnTransformer
    feature_names = pipe_lin.named_steps['prep'].get_feature_names_out()
    coeficientes = pd.DataFrame({
        'Feature': feature_names,
        'Coeficiente': pipe_lin.named_steps['reg'].coef_
    }).sort_values(by='Coeficiente', ascending=False)
    
    print(f"\nIntercepto: {pipe_lin.named_steps['reg'].intercept_:.4f}")
    print("Coeficientes de Regresión Lineal:")
    print(coeficientes.to_string(index=False))
    
    metricas_reg = {
        'R2': r2_test,
        'MAE': mae_test,
        'RMSE': rmse_test,
        'R2_train': r2_train,
        'MAE_train': mae_train,
        'RMSE_train': rmse_train
    }
    
    # 6. Generación opcional de gráficas comparativas
    if guardar_graficas:
        generar_graficas_comparativas(
            df_res_clf=df_res_clf,
            cms_clf=cms_clf,
            y_test_clf=y_test_clf,
            preds_clf=preds_clf,
            probs_clf=probs_clf,
            y_test_reg=y_test_reg,
            pred_reg=y_pred_reg,
            metricas_reg=metricas_reg,
            coeficientes_reg=coeficientes,
            output_dir=output_graficas
        )
        
    # 7. CAPA DE DECISIÓN SIMPLE (Reglas de Negocio)
    print("\n" + "=" * 75)
    print("CAPA DE DECISIÓN: REGLAS DE NEGOCIO (Basado en Probs. de Regresión Logística)")
    print("=" * 75)
    
    probs_lr_test = probs_clf['Regresión Logística (Baseline)']
    df_decisiones = aplicar_capa_decision(probs_lr_test, umbral_bajo=0.35, umbral_alto=0.65)
    resumen_dec = generar_resumen_decisiones(df_decisiones)
    
    print("Distribución de acciones sugeridas (Test 2025, N=120):")
    print(resumen_dec.to_string(index=False))
    
    # Guardar ejemplo de decisiones
    df_test_decisiones = X_test.copy()
    df_test_decisiones['riesgo_real'] = y_test_clf
    df_test_decisiones['prob_riesgo'] = df_decisiones['Probabilidad'].values
    df_test_decisiones['nivel_riesgo'] = df_decisiones['Nivel_Riesgo'].values
    df_test_decisiones['accion'] = df_decisiones['Accion_Recomendada'].values
    ruta_decisiones = "data/processed/ejemplo_decisiones_test_2025.csv"
    df_test_decisiones.to_csv(ruta_decisiones, index=False)
    print(f"\nEjemplo de decisiones exportado a: {ruta_decisiones}")
        
    return {
        'resultados_clasificacion': df_res_clf,
        'metricas_regresion': metricas_reg,
        'coeficientes_regresion': coeficientes,
        'modelos': modelos_entrenados,
        'decisiones': df_decisiones
    }


if __name__ == "__main__":
    entrenar_y_evaluar()