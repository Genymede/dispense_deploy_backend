import { Router, Request, Response, NextFunction } from 'express';
import { login, logout, me, getUsers, updateMe } from '../controllers/auth.controller';
import { getDrugs, getDrugById, getLots, createDrug, updateDrug, deleteDrug, getCategories, getMedTable } from '../controllers/drugs.controller';
import { getTransactions, stockIn, receiveStock, adjustStock, returnStock, markExpired, getStockSummary, getLotsReport, getPendingStockIn, approveStockIn, rejectStockIn } from '../controllers/stock.controller';
import { getPrescriptions, getPrescriptionById, getPrescriptionFull, createPrescription, dispensePrescription, returnPrescription, cancelPrescription, getWards, searchPatients, safetyCheck, liveSafetyCheck, createMockPrescription, updatePrescriptionItems, updatePrescriptionMeta } from '../controllers/dispense.controller';
import { getDashboardStats, getStockSummary as getDashSummary, getAlerts, markAlertRead, markAllAlertsRead, getSettings, updateSettings } from '../controllers/dashboard.controller';
import { getStockReport, getDispenseReport, getInventoryReport, getTopDrugs, getByCategory, getByWard, exportExcel, exportPdf } from '../controllers/reports.controller';
import {
  getMedRegistry, getMedRegistryById, getMedCategories,
  createMedRegistry, updateMedRegistry, deleteMedRegistry,
  getAllergyRegistry, createAllergy, updateAllergy, deleteAllergy,
  getAdrRegistry, createAdr, updateAdr, deleteAdr,
  getMedInteractions, createInteraction, updateInteraction, deleteInteraction,
  getMedUsage, getDispenseHistory, getMedDelivery,
  getOverdueMed, updateOverdueMed,
  getRadRegistry, createRadRequest, updateRadRequest, deleteRadRequest,
  getMedError, searchPatientsRegistry, searchMedTableRegistry,
  createMedUsage, updateMedUsage,
  createDelivery, updateDelivery, deleteDelivery,
  createOverdue, updateOverdue, deleteOverdue,
  getPatients, getPatientById,
  getMedProblems, createMedProblem, updateMedProblem, deleteMedProblem,
} from '../controllers/registry.controller';
import {
  reportMedTable, reportMedSubwarehouse, reportMedExpired,
  reportMedOrderHistory, reportMedUsageHistory,
  reportMedError, reportMedProblem, reportMedDelivery,
  reportOverdueMed, reportCutOff, reportRadRegistry,
  executeCutOff,
} from '../controllers/reports_extra.controller';
import { printLabel, getPrinters } from '../controllers/printer.controller';
import {
  getQueue, getQueueDisplay, getQueueStats,
  createQueue, callQueue, completeQueue, skipQueue, deleteQueue, resetQueue, receiveQueue,
} from '../controllers/queue.controller';

const r = Router();

function requireApiKey(req: Request, res: Response, next: NextFunction) {
  const key = req.headers['x-api-key'];
  if (!key || key !== process.env.MAIN_WAREHOUSE_API_KEY)
    return res.status(401).json({ error: 'Invalid or missing API key' });
  next();
}

// ── Printer ───────────────────────────────────────────────────────────────────
r.post('/print',    printLabel);
r.get('/printers',  getPrinters);

// ── Auth ──────────────────────────────────────────────────────────────────────
r.post('/auth/login',    login);
r.post('/auth/logout',   logout);
r.get('/auth/me',        me);
r.put('/auth/me',        updateMe);
r.get('/auth/users',     getUsers);

// ── Health ─────────────────────────────────────────────────────────────────────
r.get('/health', (_, res) => res.json({ status: 'ok', schema: process.env.DB_SCHEMA || 'dispense_02', time: new Date() }));

// ── Drugs (subwarehouse) ────────────────────────────────────────────────────────
r.get('/drugs',             getDrugs);
r.get('/drugs/categories',  getCategories);
r.get('/drugs/med-table',   getMedTable);
r.get('/drugs/:med_sid/lots', getLots);
r.get('/drugs/:med_sid',    getDrugById);
r.post('/drugs',            createDrug);
r.put('/drugs/:med_sid',    updateDrug);
r.delete('/drugs/:med_sid', deleteDrug);

// ── Stock ───────────────────────────────────────────────────────────────────────
r.get('/stock/transactions',      getTransactions);
r.get('/stock/summary',           getStockSummary);
r.get('/stock/lots-report',       getLotsReport);
r.get('/stock/pending-in',        getPendingStockIn);
r.post('/stock/in',               stockIn);
r.post('/stock/from-main',        receiveStock);
r.post('/stock/adjust',           adjustStock);
r.post('/stock/return',           returnStock);
r.post('/stock/expired',          markExpired);
r.patch('/stock/:tx_id/approve',  approveStockIn);
r.patch('/stock/:tx_id/reject',   rejectStockIn);

// ── Dispense ───────────────────────────────────────────────────────────────────
r.get('/dispense',                  getPrescriptions);
r.get('/dispense/wards',            getWards);
r.get('/dispense/patients',         searchPatients);
r.post('/dispense/mock',             createMockPrescription);
r.post('/dispense/live-safety',      liveSafetyCheck);

r.get('/dispense/:id',              getPrescriptionById);
r.post('/dispense',                 createPrescription);
r.post('/dispense/:id/dispense',    dispensePrescription);
r.post('/dispense/:id/return',      returnPrescription);
r.post('/dispense/:id/cancel',      cancelPrescription);
r.get('/dispense/:id/safety-check',  safetyCheck);
r.get('/dispense/:id/full',          getPrescriptionFull);
r.put('/dispense/:id/items',         updatePrescriptionItems);
r.put('/dispense/:id/meta',          updatePrescriptionMeta);

// ── Dashboard ──────────────────────────────────────────────────────────────────
r.get('/dashboard/stats',          getDashboardStats);
r.get('/dashboard/stock-summary',  getDashSummary);

// ── Alerts ─────────────────────────────────────────────────────────────────────
r.get('/alerts',               getAlerts);
r.put('/alerts/read-all',      markAllAlertsRead);
r.put('/alerts/:med_sid/read', markAlertRead);

// ── Settings ───────────────────────────────────────────────────────────────────
r.get('/settings', getSettings);
r.put('/settings', updateSettings);

// ── Reports core ───────────────────────────────────────────────────────────────
r.get('/reports/stock',            getStockReport);
r.get('/reports/dispense',         getDispenseReport);
r.get('/reports/inventory',        getInventoryReport);
r.get('/reports/top-drugs',        getTopDrugs);
r.get('/reports/by-category',      getByCategory);
r.get('/reports/by-ward',          getByWard);
r.get('/reports/export/excel',     exportExcel);
r.get('/reports/export/pdf',       exportPdf);

// ── Reports extra ──────────────────────────────────────────────────────────────
r.get('/reports/med-table',         reportMedTable);
r.get('/reports/med-subwarehouse',  reportMedSubwarehouse);
r.get('/reports/med-expired',       reportMedExpired);

r.get('/reports/med-order-history', reportMedOrderHistory);
r.get('/reports/med-usage-history', reportMedUsageHistory);
r.get('/reports/error-medication',  reportMedError);
r.get('/reports/med-problem',       reportMedProblem);
r.get('/reports/med-delivery',      reportMedDelivery);
r.get('/reports/overdue-med',       reportOverdueMed);
r.get('/reports/cut-off',           reportCutOff);
r.post('/reports/cut-off/:id/execute', executeCutOff);
r.get('/reports/rad-registry',      reportRadRegistry);

// ── Registry helpers ───────────────────────────────────────────────────────────
r.get('/registry/categories',       getMedCategories);
r.get('/registry/search/patients',  searchPatientsRegistry);
r.get('/registry/search/drugs',     searchMedTableRegistry);

// ── Registry: ทะเบียนยาหลัก ────────────────────────────────────────────────────
r.get('/registry/drugs',             getMedRegistry);
r.get('/registry/drugs/:med_id',     getMedRegistryById);
r.post('/registry/drugs',            createMedRegistry);
r.put('/registry/drugs/:med_id',     updateMedRegistry);
r.delete('/registry/drugs/:med_id',  deleteMedRegistry);

// ── Registry: การแพ้ยา ─────────────────────────────────────────────────────────
r.get('/registry/allergy',           getAllergyRegistry);
r.post('/registry/allergy',          createAllergy);
r.put('/registry/allergy/:allr_id',  updateAllergy);
r.delete('/registry/allergy/:allr_id', deleteAllergy);

// ── Registry: ADR ──────────────────────────────────────────────────────────────
r.get('/registry/adr',              getAdrRegistry);
r.post('/registry/adr',             createAdr);
r.put('/registry/adr/:adr_id',      updateAdr);
r.delete('/registry/adr/:adr_id',   deleteAdr);

// ── Registry: ปฏิกิริยายา ──────────────────────────────────────────────────────
r.get('/registry/med-interactions',                   getMedInteractions);
r.post('/registry/med-interactions',                  createInteraction);
r.put('/registry/med-interactions/:interaction_id',   updateInteraction);
r.delete('/registry/med-interactions/:interaction_id', deleteInteraction);

// ── Registry: อื่นๆ (read + partial update) ────────────────────────────────────
r.get('/registry/med-usage',              getMedUsage);
r.post('/registry/med-usage',             createMedUsage);
r.put('/registry/med-usage/:usage_id',    updateMedUsage);

r.get('/registry/dispense-history',       getDispenseHistory);
r.get('/registry/med-error',              getMedError);

r.get('/registry/delivery',               getMedDelivery);
r.post('/registry/delivery',              createDelivery);
r.put('/registry/delivery/:delivery_id',  updateDelivery);
r.delete('/registry/delivery/:delivery_id', deleteDelivery);

r.get('/registry/overdue',                getOverdueMed);
r.post('/registry/overdue',               createOverdue);
r.put('/registry/overdue/:overdue_id',    updateOverdue);
r.delete('/registry/overdue/:overdue_id', deleteOverdue);

r.get('/registry/rad',                 getRadRegistry);
r.post('/registry/rad',                createRadRequest);
r.put('/registry/rad/:rad_id',     updateRadRequest);
r.delete('/registry/rad/:rad_id',  deleteRadRequest);

r.get('/registry/med-problem',             getMedProblems);
r.post('/registry/med-problem',            createMedProblem);
r.put('/registry/med-problem/:id',         updateMedProblem);
r.delete('/registry/med-problem/:id',      deleteMedProblem);

// ── Patients ───────────────────────────────────────────────────────────────────
r.get('/patients',     getPatients);
r.get('/patients/:id', getPatientById);

// ── Queue ─────────────────────────────────────────────────────────────────────
r.get('/queue/display',       getQueueDisplay);
r.get('/queue/stats',         getQueueStats);
r.get('/queue',               getQueue);
r.post('/queue',              createQueue);
r.post('/queue/reset',        resetQueue);
r.put('/queue/:id/call',      callQueue);
r.put('/queue/:id/complete',  completeQueue);
r.put('/queue/:id/receive',   receiveQueue);
r.put('/queue/:id/skip',      skipQueue);
r.delete('/queue/:id',        deleteQueue);

export default r;
